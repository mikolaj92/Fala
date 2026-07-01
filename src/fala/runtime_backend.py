from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from fala.errors import FalaBudgetExceeded


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


class CarrierRunStatus(StrEnum):
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
    status: CarrierRunStatus = CarrierRunStatus.created
    title: str | None = None
    package_id: str | None = None
    package_version: str | None = None
    package_digest: str | None = None
    flow_id: str | None = None
    flow_digest: str | None = None
    runtime_version: str | None = None
    backend_version: str | None = None
    schema_version: int = Field(default=1, ge=1)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)
    started_at: datetime | None = None
    finished_at: datetime | None = None


class Carrier(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: _new_id("carrier"))
    run_id: str
    carrier_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)


class CarrierType(BaseModel):
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


class CarrierRelation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: _new_id("carrier_relation"))
    run_id: str
    relation_type: str
    source_carrier_id: str
    target_carrier_id: str
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
    carrier_id: str | None = None
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


class Observation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: _new_id("observation"))
    run_id: str
    kind: str
    carrier_id: str | None = None
    values: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_now)


class Artifact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: _new_id("artifact"))
    run_id: str
    kind: str
    uri: str
    carrier_id: str | None = None
    media_type: str | None = None
    size_bytes: int | None = Field(default=None, ge=0)
    content_hash: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_now)


class CarrierProcessStatus(StrEnum):
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
    carrier_id: str | None = None
    status: CarrierProcessStatus = CarrierProcessStatus.pending
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


class GateStatus(StrEnum):
    open = "open"
    completed = "completed"
    cancelled = "cancelled"
    expired = "expired"


class Gate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: _new_id("gate"))
    run_id: str
    kind: str
    carrier_id: str | None = None
    status: GateStatus = GateStatus.open
    values: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)


class CarrierWaitDiagnosticIssue(BaseModel):
    model_config = ConfigDict(extra="forbid")

    process_id: str
    status: CarrierProcessStatus | None = None
    reason: str
    blocked_by: list[str] = Field(default_factory=list)
    dependency_statuses: dict[str, str | None] = Field(default_factory=dict)
    data: dict[str, Any] = Field(default_factory=dict)


class CarrierWaitGraphDiagnostic(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    carrier_id: str | None = None
    deadlocked: bool = False
    deadlocks: list[list[str]] = Field(default_factory=list)
    wait_edges: dict[str, list[str]] = Field(default_factory=dict)
    blocked: list[CarrierWaitDiagnosticIssue] = Field(default_factory=list)
    open_gates: list[str] = Field(default_factory=list)
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
    model_config = ConfigDict(extra="forbid")

    runtime_hops: int = Field(default=0, ge=0)
    spawned_runs: int = Field(default=0, ge=0)
    carrier_count: int = Field(default=0, ge=0)
    wall_time_seconds: float = Field(default=0.0, ge=0)
    attempts: int = Field(default=0, ge=0)
    artifact_bytes: int = Field(default=0, ge=0)


class RuntimePool(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    runtimes: list[RuntimeRef] = Field(default_factory=list)
    carrier_types: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class DelegationPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: _new_id("delegation_policy"))
    pool_id: str
    carrier_types: list[str] = Field(default_factory=list)
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
    carrier: Carrier
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
        status: CarrierRunStatus,
        command: RuntimeCommand,
        events: Sequence[RuntimeEvent] = (),
    ) -> tuple[Run, CommandSubmission]: ...

    async def get_run(self, *, run_id: str) -> Run | None: ...

    async def list_runs(
        self,
        *,
        status: CarrierRunStatus | None = None,
        limit: int | None = None,
    ) -> list[Run]: ...

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

    async def put_carrier_type(self, carrier_type: CarrierType) -> None: ...

    async def register_carrier_type(
        self,
        carrier_type: CarrierType,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission: ...

    async def get_carrier_type(
        self, *, run_id: str, carrier_type_id: str
    ) -> CarrierType | None: ...

    async def list_carrier_types(self, *, run_id: str) -> list[CarrierType]: ...

    async def put_carrier(self, carrier: Carrier) -> None: ...

    async def accept_carrier(
        self,
        carrier: Carrier,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission: ...

    async def get_carrier(self, *, run_id: str, carrier_id: str) -> Carrier | None: ...

    async def list_carriers(
        self,
        *,
        run_id: str,
        carrier_type: str | None = None,
        limit: int | None = None,
    ) -> list[Carrier]: ...

    async def put_carrier_relation(self, relation: CarrierRelation) -> None: ...

    async def record_carrier_relation(
        self,
        relation: CarrierRelation,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission: ...

    async def get_carrier_relation(
        self, *, run_id: str, relation_id: str
    ) -> CarrierRelation | None: ...

    async def list_carrier_relations(
        self,
        *,
        run_id: str,
        carrier_id: str | None = None,
        relation_type: str | None = None,
    ) -> list[CarrierRelation]: ...

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
        carrier_id: str | None = None,
        after_sequence: int | None = None,
        limit: int | None = None,
    ) -> list[RuntimeEvent]: ...

    async def put_observation(self, observation: Observation) -> None: ...

    async def record_observation(
        self,
        observation: Observation,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission: ...

    async def list_observations(
        self, *, run_id: str, carrier_id: str | None = None
    ) -> list[Observation]: ...

    async def put_artifact(self, artifact: Artifact) -> None: ...

    async def record_artifact(
        self,
        artifact: Artifact,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission: ...

    async def get_artifact(self, *, run_id: str, artifact_id: str) -> Artifact | None: ...

    async def list_artifacts(
        self,
        *,
        run_id: str,
        carrier_id: str | None = None,
        kind: str | None = None,
    ) -> list[Artifact]: ...

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
        status: CarrierProcessStatus | None = None,
        carrier_id: str | None = None,
    ) -> list[Process]: ...

    async def claim_next_ready_process(
        self,
        *,
        worker_id: str,
        run_id: str | None = None,
        lease_seconds: float = 300.0,
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
        status: CarrierProcessStatus,
        command: RuntimeCommand,
        events: Sequence[RuntimeEvent] = (),
        output: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
        available_at: datetime | None = None,
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

    async def put_gate(self, gate: Gate) -> None: ...

    async def save_gate(
        self,
        gate: Gate,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission: ...

    async def get_gate(self, *, run_id: str, gate_id: str) -> Gate | None: ...

    async def transition_gate(
        self,
        *,
        run_id: str,
        gate_id: str,
        status: GateStatus,
        command: RuntimeCommand,
        events: Sequence[RuntimeEvent] = (),
        values: dict[str, Any] | None = None,
    ) -> tuple[Gate, CommandSubmission]: ...

    async def complete_gate(
        self,
        *,
        run_id: str,
        gate_id: str,
        values: dict[str, Any] | None = None,
    ) -> Gate: ...

    async def cancel_gate(
        self,
        *,
        run_id: str,
        gate_id: str,
        values: dict[str, Any] | None = None,
    ) -> Gate: ...

    async def expire_gate(
        self,
        *,
        run_id: str,
        gate_id: str,
        values: dict[str, Any] | None = None,
    ) -> Gate: ...

    async def list_gates(
        self,
        *,
        run_id: str,
        carrier_id: str | None = None,
        status: GateStatus | None = None,
    ) -> list[Gate]: ...

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
        carrier: Carrier,
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
_SQLITE_SCHEMA_VERSION = 5
SQLITE_RUNTIME_SCHEMA_VERSION = _SQLITE_SCHEMA_VERSION
_TERMINAL_RUN_STATUSES = {
    CarrierRunStatus.completed,
    CarrierRunStatus.failed,
    CarrierRunStatus.cancelled,
    CarrierRunStatus.timed_out,
}
_TERMINAL_PROCESS_STATUSES = {
    CarrierProcessStatus.succeeded,
    CarrierProcessStatus.failed,
    CarrierProcessStatus.cancelled,
    CarrierProcessStatus.timed_out,
}
_PROCESS_TRANSITION_COMMANDS = {
    CarrierProcessStatus.succeeded: "process.complete",
    CarrierProcessStatus.failed: "process.fail",
    CarrierProcessStatus.retry_wait: "process.retry",
    CarrierProcessStatus.waiting: "process.wait",
    CarrierProcessStatus.cancelled: "process.cancel",
    CarrierProcessStatus.timed_out: "process.timeout",
}
_GATE_TRANSITION_COMMANDS = {
    GateStatus.completed: "gate.complete",
    GateStatus.cancelled: "gate.cancel",
    GateStatus.expired: "gate.expire",
}
_RUN_STATUS_TRANSITIONS = {
    CarrierRunStatus.created: {
        CarrierRunStatus.active,
        CarrierRunStatus.waiting,
        CarrierRunStatus.completed,
        CarrierRunStatus.failed,
        CarrierRunStatus.cancel_requested,
        CarrierRunStatus.cancelled,
        CarrierRunStatus.timed_out,
    },
    CarrierRunStatus.active: {
        CarrierRunStatus.waiting,
        CarrierRunStatus.completed,
        CarrierRunStatus.failed,
        CarrierRunStatus.cancel_requested,
        CarrierRunStatus.cancelled,
        CarrierRunStatus.timed_out,
    },
    CarrierRunStatus.waiting: {
        CarrierRunStatus.active,
        CarrierRunStatus.completed,
        CarrierRunStatus.failed,
        CarrierRunStatus.cancel_requested,
        CarrierRunStatus.cancelled,
        CarrierRunStatus.timed_out,
    },
    CarrierRunStatus.cancel_requested: {
        CarrierRunStatus.cancelled,
        CarrierRunStatus.failed,
        CarrierRunStatus.timed_out,
    },
}


class SQLiteRuntimeBackend:
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
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    title TEXT,
                    package_id TEXT,
                    package_version TEXT,
                    package_digest TEXT,
                    flow_id TEXT,
                    flow_digest TEXT,
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

                CREATE TABLE IF NOT EXISTS carriers (
                    run_id TEXT NOT NULL,
                    id TEXT NOT NULL,
                    carrier_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    metadata TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, id)
                );

                CREATE TABLE IF NOT EXISTS carrier_types (
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

                CREATE TABLE IF NOT EXISTS carrier_relations (
                    run_id TEXT NOT NULL,
                    id TEXT NOT NULL,
                    relation_type TEXT NOT NULL,
                    source_carrier_id TEXT NOT NULL,
                    target_carrier_id TEXT NOT NULL,
                    metadata TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, id),
                    FOREIGN KEY (run_id, source_carrier_id)
                        REFERENCES carriers (run_id, id),
                    FOREIGN KEY (run_id, target_carrier_id)
                        REFERENCES carriers (run_id, id)
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
                    carrier_id TEXT,
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

                CREATE TABLE IF NOT EXISTS observations (
                    run_id TEXT NOT NULL,
                    id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    carrier_id TEXT,
                    values_json TEXT NOT NULL,
                    metadata TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, id)
                );

                CREATE TABLE IF NOT EXISTS artifacts (
                    run_id TEXT NOT NULL,
                    id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    uri TEXT NOT NULL,
                    carrier_id TEXT,
                    media_type TEXT,
                    size_bytes INTEGER,
                    content_hash TEXT,
                    metadata TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, id),
                    FOREIGN KEY (run_id, carrier_id)
                        REFERENCES carriers (run_id, id)
                );

                CREATE TABLE IF NOT EXISTS processes (
                    run_id TEXT NOT NULL,
                    id TEXT NOT NULL,
                    process_type TEXT NOT NULL,
                    carrier_id TEXT,
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
                    PRIMARY KEY (run_id, id),
                    FOREIGN KEY (run_id, carrier_id)
                        REFERENCES carriers (run_id, id)
                );

                CREATE TABLE IF NOT EXISTS gates (
                    run_id TEXT NOT NULL,
                    id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    carrier_id TEXT,
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
                    carrier_json TEXT NOT NULL,
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
                    carrier_json TEXT NOT NULL,
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

                CREATE INDEX IF NOT EXISTS idx_runtime_events_carrier
                    ON runtime_events (run_id, carrier_id, sequence);
                CREATE INDEX IF NOT EXISTS idx_runs_status
                    ON runs (status, updated_at);

                CREATE TABLE IF NOT EXISTS runtime_pools (
                    id TEXT PRIMARY KEY,
                    runtimes_json TEXT NOT NULL,
                    carrier_types TEXT NOT NULL,
                    metadata TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS delegation_policies (
                    id TEXT PRIMARY KEY,
                    pool_id TEXT NOT NULL,
                    carrier_types TEXT NOT NULL,
                    budget TEXT NOT NULL,
                    metadata TEXT NOT NULL,
                    FOREIGN KEY (pool_id)
                        REFERENCES runtime_pools (id)
                );

                CREATE INDEX IF NOT EXISTS idx_delegation_policies_pool
                    ON delegation_policies (pool_id, id);

                CREATE INDEX IF NOT EXISTS idx_carrier_relations_source
                    ON carrier_relations (run_id, source_carrier_id, relation_type);
                CREATE INDEX IF NOT EXISTS idx_carrier_relations_target
                    ON carrier_relations (run_id, target_carrier_id, relation_type);
                CREATE INDEX IF NOT EXISTS idx_observations_carrier
                    ON observations (run_id, carrier_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_artifacts_carrier
                    ON artifacts (run_id, carrier_id, kind, created_at);
                CREATE INDEX IF NOT EXISTS idx_processes_ready
                    ON processes (status, available_at, priority, created_at);
                CREATE INDEX IF NOT EXISTS idx_processes_run_status
                    ON processes (run_id, status, updated_at);
                CREATE INDEX IF NOT EXISTS idx_processes_carrier
                    ON processes (run_id, carrier_id, status);
                CREATE INDEX IF NOT EXISTS idx_gates_status
                    ON gates (run_id, status, updated_at);
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
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO runs (
                        id, status, title, package_id, package_version,
                        package_digest, flow_id, flow_digest, runtime_version,
                        backend_version, schema_version, metadata, created_at,
                        updated_at, started_at, finished_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        status = excluded.status,
                        title = excluded.title,
                        package_id = excluded.package_id,
                        package_version = excluded.package_version,
                        package_digest = excluded.package_digest,
                        flow_id = excluded.flow_id,
                        flow_digest = excluded.flow_digest,
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
        status: CarrierRunStatus,
        command: RuntimeCommand,
        events: Sequence[RuntimeEvent] = (),
    ) -> tuple[Run, CommandSubmission]:
        if command.run_id != run_id:
            raise ValueError("run transition command run_id must match run_id")
        if command.command_type == "run.cancel":
            if status != CarrierRunStatus.cancel_requested:
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
                    now if status == CarrierRunStatus.active else None
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
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM runs WHERE id = ?",
                (run_id,),
            ).fetchone()
        return _run_from_row(row) if row is not None else None

    async def list_runs(
        self,
        *,
        status: CarrierRunStatus | None = None,
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
        with self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [_run_from_row(row) for row in rows]

    async def put_runtime_pool(self, pool: RuntimePool) -> None:
        async with self._lock:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO runtime_pools (
                        id, runtimes_json, carrier_types, metadata
                    ) VALUES (?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        runtimes_json = excluded.runtimes_json,
                        carrier_types = excluded.carrier_types,
                        metadata = excluded.metadata
                    """,
                    _runtime_pool_args(pool),
                )
                connection.commit()

    async def get_runtime_pool(self, *, pool_id: str) -> RuntimePool | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM runtime_pools WHERE id = ?",
                (pool_id,),
            ).fetchone()
        return _runtime_pool_from_row(row) if row is not None else None

    async def list_runtime_pools(self) -> list[RuntimePool]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM runtime_pools
                ORDER BY id ASC
                """
            ).fetchall()
        return [_runtime_pool_from_row(row) for row in rows]

    async def put_delegation_policy(self, policy: DelegationPolicy) -> None:
        async with self._lock:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO delegation_policies (
                        id, pool_id, carrier_types, budget, metadata
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        pool_id = excluded.pool_id,
                        carrier_types = excluded.carrier_types,
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
        with self._connect() as connection:
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
        with self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [_delegation_policy_from_row(row) for row in rows]

    async def put_carrier_type(self, carrier_type: CarrierType) -> None:
        async with self._lock:
            with self._connect() as connection:
                _require_run_row(connection, carrier_type.run_id)
                connection.execute(
                    """
                    INSERT INTO carrier_types (
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
                        carrier_type.run_id,
                        carrier_type.id,
                        carrier_type.title,
                        carrier_type.description,
                        json.dumps(carrier_type.media_types),
                        _dumps(carrier_type.value_schema),
                        _dumps(carrier_type.metadata),
                        carrier_type.created_at.isoformat(),
                        carrier_type.updated_at.isoformat(),
                    ),
                )
                connection.commit()

    async def register_carrier_type(
        self,
        carrier_type: CarrierType,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != carrier_type.run_id:
            raise ValueError(
                "carrier_type.register command run_id must match carrier type run_id"
            )
        if command.command_type != "carrier_type.register":
            raise ValueError(
                "register_carrier_type requires command_type 'carrier_type.register'"
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
                _require_run_row(connection, carrier_type.run_id)
                if (
                    connection.execute(
                        "SELECT 1 FROM carrier_types WHERE run_id = ? AND id = ?",
                        (carrier_type.run_id, carrier_type.id),
                    ).fetchone()
                    is not None
                ):
                    raise ValueError(f"Carrier type already exists: {carrier_type.id!r}")

                _insert_runtime_command_row(connection, command)
                _insert_carrier_type_row(connection, carrier_type)
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

    async def get_carrier_type(
        self,
        *,
        run_id: str,
        carrier_type_id: str,
    ) -> CarrierType | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM carrier_types WHERE run_id = ? AND id = ?",
                (run_id, carrier_type_id),
            ).fetchone()
        return _carrier_type_from_row(row) if row is not None else None

    async def list_carrier_types(self, *, run_id: str) -> list[CarrierType]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM carrier_types
                WHERE run_id = ?
                ORDER BY id ASC
                """,
                (run_id,),
            ).fetchall()
        return [_carrier_type_from_row(row) for row in rows]

    async def put_carrier(self, carrier: Carrier) -> None:
        async with self._lock:
            with self._connect() as connection:
                _require_run_row(connection, carrier.run_id)
                connection.execute(
                    """
                    INSERT INTO carriers (
                        run_id, id, carrier_type, payload, metadata,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, id) DO UPDATE SET
                        carrier_type = excluded.carrier_type,
                        payload = excluded.payload,
                        metadata = excluded.metadata,
                        created_at = excluded.created_at,
                        updated_at = excluded.updated_at
                    """,
                    (
                        carrier.run_id,
                        carrier.id,
                        carrier.carrier_type,
                        _dumps(carrier.payload),
                        _dumps(carrier.metadata),
                        carrier.created_at.isoformat(),
                        carrier.updated_at.isoformat(),
                    ),
                )
                connection.commit()

    async def accept_carrier(
        self,
        carrier: Carrier,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != carrier.run_id:
            raise ValueError("carrier.accept command run_id must match carrier run_id")
        if command.command_type != "carrier.accept":
            raise ValueError("accept_carrier requires command_type 'carrier.accept'")
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
                _require_run_row(connection, carrier.run_id)
                if (
                    connection.execute(
                        "SELECT 1 FROM carriers WHERE run_id = ? AND id = ?",
                        (carrier.run_id, carrier.id),
                    ).fetchone()
                    is not None
                ):
                    raise ValueError(f"Carrier already exists: {carrier.id!r}")

                _insert_runtime_command_row(connection, command)
                _insert_carrier_row(connection, carrier)
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

    async def get_carrier(self, *, run_id: str, carrier_id: str) -> Carrier | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM carriers WHERE run_id = ? AND id = ?",
                (run_id, carrier_id),
            ).fetchone()
        return _carrier_from_row(row) if row is not None else None

    async def list_carriers(
        self,
        *,
        run_id: str,
        carrier_type: str | None = None,
        limit: int | None = None,
    ) -> list[Carrier]:
        clauses = ["run_id = ?"]
        params: list[Any] = [run_id]
        if carrier_type is not None:
            clauses.append("carrier_type = ?")
            params.append(carrier_type)
        sql = f"""
            SELECT * FROM carriers
            WHERE {' AND '.join(clauses)}
            ORDER BY created_at ASC, id ASC
        """
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        with self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [_carrier_from_row(row) for row in rows]

    async def put_carrier_relation(self, relation: CarrierRelation) -> None:
        async with self._lock:
            with self._connect() as connection:
                _require_run_row(connection, relation.run_id)
                connection.execute(
                    """
                    INSERT INTO carrier_relations (
                        run_id, id, relation_type, source_carrier_id,
                        target_carrier_id, metadata, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, id) DO UPDATE SET
                        relation_type = excluded.relation_type,
                        source_carrier_id = excluded.source_carrier_id,
                        target_carrier_id = excluded.target_carrier_id,
                        metadata = excluded.metadata,
                        created_at = excluded.created_at
                    """,
                    (
                        relation.run_id,
                        relation.id,
                        relation.relation_type,
                        relation.source_carrier_id,
                        relation.target_carrier_id,
                        _dumps(relation.metadata),
                        relation.created_at.isoformat(),
                    ),
                )
                connection.commit()

    async def record_carrier_relation(
        self,
        relation: CarrierRelation,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != relation.run_id:
            raise ValueError(
                "carrier_relation.record command run_id must match relation run_id"
            )
        if command.command_type != "carrier_relation.record":
            raise ValueError(
                "record_carrier_relation requires command_type "
                "'carrier_relation.record'"
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
                        "SELECT 1 FROM carrier_relations WHERE run_id = ? AND id = ?",
                        (relation.run_id, relation.id),
                    ).fetchone()
                    is not None
                ):
                    raise ValueError(
                        f"Carrier relation already exists: {relation.id!r}"
                    )

                _insert_runtime_command_row(connection, command)
                _insert_carrier_relation_row(connection, relation)
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

    async def get_carrier_relation(
        self,
        *,
        run_id: str,
        relation_id: str,
    ) -> CarrierRelation | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM carrier_relations WHERE run_id = ? AND id = ?",
                (run_id, relation_id),
            ).fetchone()
        return _carrier_relation_from_row(row) if row is not None else None

    async def list_carrier_relations(
        self,
        *,
        run_id: str,
        carrier_id: str | None = None,
        relation_type: str | None = None,
    ) -> list[CarrierRelation]:
        clauses = ["run_id = ?"]
        params: list[Any] = [run_id]
        if carrier_id is not None:
            clauses.append("(source_carrier_id = ? OR target_carrier_id = ?)")
            params.extend([carrier_id, carrier_id])
        if relation_type is not None:
            clauses.append("relation_type = ?")
            params.append(relation_type)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM carrier_relations
                WHERE {' AND '.join(clauses)}
                ORDER BY created_at ASC, id ASC
                """,
                params,
            ).fetchall()
        return [_carrier_relation_from_row(row) for row in rows]

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
        with self._connect() as connection:
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
        with self._connect() as connection:
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
        with self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [_command_from_row(row) for row in rows]

    async def list_events(
        self,
        *,
        run_id: str,
        carrier_id: str | None = None,
        after_sequence: int | None = None,
        limit: int | None = None,
    ) -> list[RuntimeEvent]:
        clauses = ["run_id = ?"]
        params: list[Any] = [run_id]
        if carrier_id is not None:
            clauses.append("carrier_id = ?")
            params.append(carrier_id)
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
        with self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [_event_from_row(row) for row in rows]

    async def put_observation(self, observation: Observation) -> None:
        async with self._lock:
            with self._connect() as connection:
                _require_run_row(connection, observation.run_id)
                connection.execute(
                    """
                    INSERT INTO observations (
                        run_id, id, kind, carrier_id, values_json,
                        metadata, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, id) DO UPDATE SET
                        kind = excluded.kind,
                        carrier_id = excluded.carrier_id,
                        values_json = excluded.values_json,
                        metadata = excluded.metadata,
                        created_at = excluded.created_at
                    """,
                    (
                        observation.run_id,
                        observation.id,
                        observation.kind,
                        observation.carrier_id,
                        _dumps(observation.values),
                        _dumps(observation.metadata),
                        observation.created_at.isoformat(),
                    ),
                )
                connection.commit()

    async def record_observation(
        self,
        observation: Observation,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != observation.run_id:
            raise ValueError(
                "observation.record command run_id must match observation run_id"
            )
        if command.command_type != "observation.record":
            raise ValueError(
                "record_observation requires command_type 'observation.record'"
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
                _require_run_row(connection, observation.run_id)
                if (
                    connection.execute(
                        "SELECT 1 FROM observations WHERE run_id = ? AND id = ?",
                        (observation.run_id, observation.id),
                    ).fetchone()
                    is not None
                ):
                    raise ValueError(f"Observation already exists: {observation.id!r}")

                _insert_runtime_command_row(connection, command)
                _insert_observation_row(connection, observation)
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

    async def list_observations(
        self, *, run_id: str, carrier_id: str | None = None
    ) -> list[Observation]:
        clauses = ["run_id = ?"]
        params: list[Any] = [run_id]
        if carrier_id is not None:
            clauses.append("carrier_id = ?")
            params.append(carrier_id)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM observations
                WHERE {' AND '.join(clauses)}
                ORDER BY created_at ASC, id ASC
                """,
                params,
            ).fetchall()
        return [_observation_from_row(row) for row in rows]

    async def put_artifact(self, artifact: Artifact) -> None:
        async with self._lock:
            with self._connect() as connection:
                _require_run_row(connection, artifact.run_id)
                connection.execute(
                    """
                    INSERT INTO artifacts (
                        run_id, id, kind, uri, carrier_id, media_type,
                        size_bytes, content_hash, metadata, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, id) DO NOTHING
                    """,
                    (
                        artifact.run_id,
                        artifact.id,
                        artifact.kind,
                        artifact.uri,
                        artifact.carrier_id,
                        artifact.media_type,
                        artifact.size_bytes,
                        artifact.content_hash,
                        _dumps(artifact.metadata),
                        artifact.created_at.isoformat(),
                    ),
                )
                connection.commit()

    async def record_artifact(
        self,
        artifact: Artifact,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != artifact.run_id:
            raise ValueError(
                "artifact.record command run_id must match artifact run_id"
            )
        if command.command_type != "artifact.record":
            raise ValueError("record_artifact requires command_type 'artifact.record'")
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
                _require_run_row(connection, artifact.run_id)
                if (
                    connection.execute(
                        "SELECT 1 FROM artifacts WHERE run_id = ? AND id = ?",
                        (artifact.run_id, artifact.id),
                    ).fetchone()
                    is not None
                ):
                    raise ValueError(f"Artifact already exists: {artifact.id!r}")

                _insert_runtime_command_row(connection, command)
                _insert_artifact_row(connection, artifact)
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

    async def get_artifact(self, *, run_id: str, artifact_id: str) -> Artifact | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM artifacts WHERE run_id = ? AND id = ?",
                (run_id, artifact_id),
            ).fetchone()
        return _artifact_from_row(row) if row is not None else None

    async def list_artifacts(
        self,
        *,
        run_id: str,
        carrier_id: str | None = None,
        kind: str | None = None,
    ) -> list[Artifact]:
        clauses = ["run_id = ?"]
        params: list[Any] = [run_id]
        if carrier_id is not None:
            clauses.append("carrier_id = ?")
            params.append(carrier_id)
        if kind is not None:
            clauses.append("kind = ?")
            params.append(kind)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM artifacts
                WHERE {' AND '.join(clauses)}
                ORDER BY created_at ASC, id ASC
                """,
                params,
            ).fetchall()
        return [_artifact_from_row(row) for row in rows]

    async def put_process(self, process: Process) -> None:
        async with self._lock:
            with self._connect() as connection:
                _require_run_row(connection, process.run_id)
                connection.execute(
                    """
                    INSERT INTO processes (
                        run_id, id, process_type, carrier_id, status, priority,
                        attempt, max_attempts, available_at, lease_owner,
                        lease_expires_at, input_json, output_json, error_json,
                        metadata, created_at, updated_at, started_at, finished_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, id) DO UPDATE SET
                        process_type = excluded.process_type,
                        carrier_id = excluded.carrier_id,
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
                    CarrierProcessStatus.pending,
                    CarrierProcessStatus.ready,
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
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM processes WHERE run_id = ? AND id = ?",
                (run_id, process_id),
            ).fetchone()
        return _process_from_row(row) if row is not None else None

    async def list_processes(
        self,
        *,
        run_id: str,
        status: CarrierProcessStatus | None = None,
        carrier_id: str | None = None,
    ) -> list[Process]:
        clauses = ["run_id = ?"]
        params: list[Any] = [run_id]
        if status is not None:
            clauses.append("status = ?")
            params.append(status.value)
        if carrier_id is not None:
            clauses.append("carrier_id = ?")
            params.append(carrier_id)
        with self._connect() as connection:
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
    ) -> Process | None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be greater than zero")
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
            CarrierProcessStatus.ready.value,
            CarrierProcessStatus.retry_wait.value,
            now.isoformat(),
            CarrierProcessStatus.running.value,
            now.isoformat(),
        ]
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(run_id)
        async with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
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
                        CarrierProcessStatus.running.value,
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
            status=CarrierProcessStatus.succeeded,
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
            status=CarrierProcessStatus.failed,
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
            status=CarrierProcessStatus.cancelled,
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
            status=CarrierProcessStatus.timed_out,
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
                    CarrierProcessStatus.running,
                    CarrierProcessStatus.failed,
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
                        CarrierProcessStatus.retry_wait.value,
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
        status: CarrierProcessStatus,
        command: RuntimeCommand,
        events: Sequence[RuntimeEvent] = (),
        output: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
        available_at: datetime | None = None,
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
                    CarrierProcessStatus.succeeded,
                    CarrierProcessStatus.failed,
                }:
                    if process.status != CarrierProcessStatus.running:
                        raise ValueError(
                            f"Process {process_id!r} is not running: "
                            f"{process.status.value}"
                        )
                    stored_output = (
                        output or {}
                        if status == CarrierProcessStatus.succeeded
                        else {}
                    )
                    stored_error = (
                        error or {}
                        if status == CarrierProcessStatus.failed
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
                elif status == CarrierProcessStatus.retry_wait:
                    if process.status not in {
                        CarrierProcessStatus.running,
                        CarrierProcessStatus.failed,
                    }:
                        raise ValueError(
                            f"Process {process_id!r} cannot be retried from status: "
                            f"{process.status.value}"
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
                            CarrierProcessStatus.retry_wait.value,
                            (available_at or now).isoformat(),
                            _dumps(error or process.error),
                            now.isoformat(),
                            run_id,
                            process_id,
                        ),
                    )
                elif status == CarrierProcessStatus.waiting:
                    if process.status != CarrierProcessStatus.running:
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
                            CarrierProcessStatus.waiting.value,
                            _dumps(output or process.output),
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
        status: CarrierProcessStatus,
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
        status: CarrierProcessStatus,
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
                if process.status != CarrierProcessStatus.running:
                    raise ValueError(
                        f"Process {process_id!r} is not running: {process.status.value}"
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

    async def put_gate(self, gate: Gate) -> None:
        async with self._lock:
            with self._connect() as connection:
                _require_run_row(connection, gate.run_id)
                connection.execute(
                    """
                    INSERT INTO gates (
                        run_id, id, kind, carrier_id, status, values_json,
                        metadata, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, id) DO UPDATE SET
                        kind = excluded.kind,
                        carrier_id = excluded.carrier_id,
                        status = excluded.status,
                        values_json = excluded.values_json,
                        metadata = excluded.metadata,
                        created_at = excluded.created_at,
                        updated_at = excluded.updated_at
                    """,
                    (
                        gate.run_id,
                        gate.id,
                        gate.kind,
                        gate.carrier_id,
                        gate.status.value,
                        _dumps(gate.values),
                        _dumps(gate.metadata),
                        gate.created_at.isoformat(),
                        gate.updated_at.isoformat(),
                    ),
                )
                connection.commit()

    async def save_gate(
        self,
        gate: Gate,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != gate.run_id:
            raise ValueError("gate command run_id must match gate run_id")
        if command.command_type not in {"gate.save", "gate.open"}:
            raise ValueError(
                "save_gate requires command_type 'gate.save' or 'gate.open'"
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
                _require_run_row(connection, gate.run_id)
                if (
                    connection.execute(
                        "SELECT 1 FROM gates WHERE run_id = ? AND id = ?",
                        (gate.run_id, gate.id),
                    ).fetchone()
                    is not None
                ):
                    raise ValueError(f"Gate already exists: {gate.id!r}")

                _insert_runtime_command_row(connection, command)
                _insert_gate_row(connection, gate)
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

    async def get_gate(self, *, run_id: str, gate_id: str) -> Gate | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM gates WHERE run_id = ? AND id = ?",
                (run_id, gate_id),
            ).fetchone()
        return _gate_from_row(row) if row is not None else None

    async def transition_gate(
        self,
        *,
        run_id: str,
        gate_id: str,
        status: GateStatus,
        command: RuntimeCommand,
        events: Sequence[RuntimeEvent] = (),
        values: dict[str, Any] | None = None,
    ) -> tuple[Gate, CommandSubmission]:
        expected_command_type = _GATE_TRANSITION_COMMANDS.get(status)
        if expected_command_type is None:
            raise ValueError(f"Unsupported gate terminal status: {status.value}")
        if command.run_id != run_id:
            raise ValueError("gate transition command run_id must match run_id")
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
                    stored_gate_id = stored_command.payload.get("gate_id", gate_id)
                    stored_gate = connection.execute(
                        "SELECT * FROM gates WHERE run_id = ? AND id = ?",
                        (run_id, str(stored_gate_id)),
                    ).fetchone()
                    if stored_gate is None:
                        raise ValueError(
                            "Replayed gate transition has no stored gate: "
                            f"{stored_gate_id!r}"
                        )
                    connection.commit()
                    return (
                        _gate_from_row(stored_gate),
                        CommandSubmission(
                            command=stored_command,
                            events=[],
                            replayed=True,
                        ),
                    )

                row = connection.execute(
                    "SELECT * FROM gates WHERE run_id = ? AND id = ?",
                    (run_id, gate_id),
                ).fetchone()
                if row is None:
                    raise ValueError(f"Unknown gate: {gate_id!r}")
                gate = _gate_from_row(row)
                if gate.status != GateStatus.open:
                    raise ValueError(
                        f"Gate {gate_id!r} is not open: {gate.status.value}"
                    )

                now = _now()
                _insert_runtime_command_row(connection, command)
                connection.execute(
                    """
                    UPDATE gates
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
                        gate_id,
                    ),
                )
                updated = connection.execute(
                    "SELECT * FROM gates WHERE run_id = ? AND id = ?",
                    (run_id, gate_id),
                ).fetchone()
                stored_events = _append_runtime_events(connection, command, events)
                connection.commit()
                return (
                    _gate_from_row(updated),
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

    async def complete_gate(
        self,
        *,
        run_id: str,
        gate_id: str,
        values: dict[str, Any] | None = None,
    ) -> Gate:
        return await self._finish_gate(
            run_id=run_id,
            gate_id=gate_id,
            status=GateStatus.completed,
            values=values,
        )

    async def cancel_gate(
        self,
        *,
        run_id: str,
        gate_id: str,
        values: dict[str, Any] | None = None,
    ) -> Gate:
        return await self._finish_gate(
            run_id=run_id,
            gate_id=gate_id,
            status=GateStatus.cancelled,
            values=values,
        )

    async def expire_gate(
        self,
        *,
        run_id: str,
        gate_id: str,
        values: dict[str, Any] | None = None,
    ) -> Gate:
        return await self._finish_gate(
            run_id=run_id,
            gate_id=gate_id,
            status=GateStatus.expired,
            values=values,
        )

    async def _finish_gate(
        self,
        *,
        run_id: str,
        gate_id: str,
        status: GateStatus,
        values: dict[str, Any] | None,
    ) -> Gate:
        async with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT * FROM gates WHERE run_id = ? AND id = ?",
                    (run_id, gate_id),
                ).fetchone()
                if row is None:
                    raise ValueError(f"Unknown gate: {gate_id!r}")
                gate = _gate_from_row(row)
                if gate.status != GateStatus.open:
                    raise ValueError(
                        f"Gate {gate_id!r} is not open: {gate.status.value}"
                    )
                now = _now()
                connection.execute(
                    """
                    UPDATE gates
                    SET status = ?,
                        values_json = ?,
                        updated_at = ?
                    WHERE run_id = ? AND id = ?
                    """,
                    (
                        status.value,
                        _dumps(values if values is not None else gate.values),
                        now.isoformat(),
                        run_id,
                        gate_id,
                    ),
                )
                updated = connection.execute(
                    "SELECT * FROM gates WHERE run_id = ? AND id = ?",
                    (run_id, gate_id),
                ).fetchone()
                connection.commit()
                return _gate_from_row(updated)
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    async def list_gates(
        self,
        *,
        run_id: str,
        carrier_id: str | None = None,
        status: GateStatus | None = None,
    ) -> list[Gate]:
        clauses = ["run_id = ?"]
        params: list[Any] = [run_id]
        if carrier_id is not None:
            clauses.append("carrier_id = ?")
            params.append(carrier_id)
        if status is not None:
            clauses.append("status = ?")
            params.append(status.value)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM gates
                WHERE {' AND '.join(clauses)}
                ORDER BY updated_at ASC, id ASC
                """,
                params,
            ).fetchall()
        return [_gate_from_row(row) for row in rows]

    async def put_projection(self, projection: Projection) -> None:
        async with self._lock:
            with self._connect() as connection:
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
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM projections WHERE run_id = ? AND name = ?",
                (run_id, name),
            ).fetchone()
        return _projection_from_row(row) if row is not None else None

    async def list_projections(self, *, run_id: str) -> list[Projection]:
        with self._connect() as connection:
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
            with self._connect() as connection:
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
        carrier: Carrier,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.command_type != "bridge.inbox.import":
            raise ValueError(
                "import_inbox_delivery requires command_type 'bridge.inbox.import'"
            )
        if carrier.run_id != delivery.run_id:
            raise ValueError("imported carrier run_id must match delivery run_id")
        return await self._submit_bridge_delivery(
            "bridge_inbox",
            delivery,
            command,
            events=events,
            carrier=carrier,
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
            with self._connect() as connection:
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
        carrier: Carrier | None = None,
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
                _insert_runtime_command_row(connection, command)
                if carrier is not None:
                    _upsert_carrier_row(connection, carrier)
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
        with self._connect() as connection:
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
        with self._connect() as connection:
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
        return cls(SQLiteRuntimeBackend(path))

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
        status: CarrierRunStatus,
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
            status=CarrierRunStatus.cancel_requested,
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
        status: CarrierRunStatus,
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

    async def register_carrier_type(
        self,
        carrier_type: CarrierType,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[CarrierType, CommandSubmission]:
        command = RuntimeCommand(
            run_id=carrier_type.run_id,
            command_type="carrier_type.register",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={"carrier_type_id": carrier_type.id},
        )
        event = RuntimeEvent(
            run_id=carrier_type.run_id,
            event_type="carrier_type.registered",
            payload={"carrier_type_id": carrier_type.id},
        )
        submission = await self.backend.register_carrier_type(
            carrier_type,
            command,
            events=[event],
        )
        if submission.replayed:
            existing_type_id = submission.command.payload.get(
                "carrier_type_id",
                carrier_type.id,
            )
            existing = await self.backend.get_carrier_type(
                run_id=carrier_type.run_id,
                carrier_type_id=str(existing_type_id),
            )
            if existing is None:
                raise ValueError(
                    "Replayed carrier type command has no stored carrier type: "
                    f"{existing_type_id!r}"
            )
            return existing, submission

        return carrier_type, submission

    async def accept_carrier(
        self,
        carrier: Carrier,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Carrier, CommandSubmission]:
        command = RuntimeCommand(
            run_id=carrier.run_id,
            command_type="carrier.accept",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={"carrier_id": carrier.id, "carrier_type": carrier.carrier_type},
        )
        event = RuntimeEvent(
            run_id=carrier.run_id,
            carrier_id=carrier.id,
            event_type="carrier.accepted",
            payload={"carrier_type": carrier.carrier_type},
        )
        submission = await self.backend.accept_carrier(
            carrier,
            command,
            events=[event],
        )
        if submission.replayed:
            existing_carrier_id = submission.command.payload.get("carrier_id", carrier.id)
            existing = await self.backend.get_carrier(
                run_id=carrier.run_id,
                carrier_id=str(existing_carrier_id),
            )
            if existing is None:
                raise ValueError(
                    "Replayed carrier acceptance command has no stored carrier: "
                    f"{existing_carrier_id!r}"
                )
            return existing, submission

        return carrier, submission

    async def record_carrier_relation(
        self,
        relation: CarrierRelation,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[CarrierRelation, CommandSubmission]:
        command = RuntimeCommand(
            run_id=relation.run_id,
            command_type="carrier_relation.record",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={
                "relation_id": relation.id,
                "relation_type": relation.relation_type,
                "source_carrier_id": relation.source_carrier_id,
                "target_carrier_id": relation.target_carrier_id,
            },
        )
        event = RuntimeEvent(
            run_id=relation.run_id,
            carrier_id=relation.source_carrier_id,
            event_type="carrier_relation.recorded",
            payload={
                "relation_id": relation.id,
                "relation_type": relation.relation_type,
                "target_carrier_id": relation.target_carrier_id,
            },
        )
        submission = await self.backend.record_carrier_relation(
            relation,
            command,
            events=[event],
        )
        if submission.replayed:
            existing_relation_id = submission.command.payload.get(
                "relation_id",
                relation.id,
            )
            existing = await self.backend.get_carrier_relation(
                run_id=relation.run_id,
                relation_id=str(existing_relation_id),
            )
            if existing is None:
                raise ValueError(
                    "Replayed carrier relation command has no stored relation: "
                    f"{existing_relation_id!r}"
            )
            return existing, submission

        return relation, submission

    async def record_observation(
        self,
        observation: Observation,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Observation, CommandSubmission]:
        command = RuntimeCommand(
            run_id=observation.run_id,
            command_type="observation.record",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={"observation_id": observation.id, "kind": observation.kind},
        )
        event = RuntimeEvent(
            run_id=observation.run_id,
            carrier_id=observation.carrier_id,
            event_type="observation.recorded",
            payload={"observation_id": observation.id, "kind": observation.kind},
        )
        submission = await self.backend.record_observation(
            observation,
            command,
            events=[event],
        )
        if submission.replayed:
            observations = await self.backend.list_observations(
                run_id=observation.run_id,
                carrier_id=observation.carrier_id,
            )
            for existing in observations:
                if existing.id == submission.command.payload.get("observation_id"):
                    return existing, submission
            raise ValueError(
                "Replayed observation command has no stored observation: "
                f"{submission.command.payload.get('observation_id')!r}"
            )

        return observation, submission

    async def record_artifact(
        self,
        artifact: Artifact,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Artifact, CommandSubmission]:
        command = RuntimeCommand(
            run_id=artifact.run_id,
            command_type="artifact.record",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={
                "artifact_id": artifact.id,
                "kind": artifact.kind,
                "uri": artifact.uri,
            },
        )
        event = RuntimeEvent(
            run_id=artifact.run_id,
            carrier_id=artifact.carrier_id,
            event_type="artifact.recorded",
            payload={
                "artifact_id": artifact.id,
                "kind": artifact.kind,
                "uri": artifact.uri,
            },
        )
        submission = await self.backend.record_artifact(
            artifact,
            command,
            events=[event],
        )
        if submission.replayed:
            existing_artifact_id = submission.command.payload.get(
                "artifact_id",
                artifact.id,
            )
            existing = await self.backend.get_artifact(
                run_id=artifact.run_id,
                artifact_id=str(existing_artifact_id),
            )
            if existing is None:
                raise ValueError(
                    "Replayed artifact command has no stored artifact: "
                    f"{existing_artifact_id!r}"
                )
            return existing, submission

        return artifact, submission

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
            CarrierProcessStatus.pending,
            CarrierProcessStatus.ready,
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
                "carrier_id": process.carrier_id,
                "status": process.status.value,
            },
        )
        event = RuntimeEvent(
            run_id=process.run_id,
            carrier_id=process.carrier_id,
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

    async def claim_next_ready_process(
        self,
        *,
        worker_id: str,
        run_id: str | None = None,
        lease_seconds: float = 300.0,
    ) -> Process | None:
        return await self.backend.claim_next_ready_process(
            worker_id=worker_id,
            run_id=run_id,
            lease_seconds=lease_seconds,
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
        if existing.status != CarrierProcessStatus.running:
            raise ValueError(
                f"Process {process_id!r} is not running: {existing.status.value}"
            )
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
            carrier_id=existing.carrier_id,
            process_id=process_id,
            event_type="process.completed",
            payload={"process_id": process_id},
        )
        return await self.backend.transition_process(
            run_id=run_id,
            process_id=process_id,
            status=CarrierProcessStatus.succeeded,
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
        if existing.status != CarrierProcessStatus.running:
            raise ValueError(
                f"Process {process_id!r} is not running: {existing.status.value}"
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
            carrier_id=existing.carrier_id,
            process_id=process_id,
            event_type="process.failed",
            payload={"process_id": process_id},
        )
        return await self.backend.transition_process(
            run_id=run_id,
            process_id=process_id,
            status=CarrierProcessStatus.failed,
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
            status=CarrierProcessStatus.cancelled,
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
            status=CarrierProcessStatus.timed_out,
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
        status: CarrierProcessStatus,
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
            carrier_id=existing.carrier_id,
            process_id=process_id,
            event_type=event_type,
            payload={"process_id": process_id},
        )
        if status == CarrierProcessStatus.cancelled:
            return await self.backend.transition_process(
                run_id=run_id,
                process_id=process_id,
                status=CarrierProcessStatus.cancelled,
                command=command,
                events=[event],
                error=error,
            )
        if status == CarrierProcessStatus.timed_out:
            return await self.backend.transition_process(
                run_id=run_id,
                process_id=process_id,
                status=CarrierProcessStatus.timed_out,
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
            CarrierProcessStatus.running,
            CarrierProcessStatus.failed,
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
            carrier_id=existing.carrier_id,
            process_id=process_id,
            event_type="process.retry_scheduled",
            payload={"process_id": process_id},
        )
        return await self.backend.transition_process(
            run_id=run_id,
            process_id=process_id,
            status=CarrierProcessStatus.retry_wait,
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
        if existing.status != CarrierProcessStatus.running:
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
            carrier_id=existing.carrier_id,
            process_id=process_id,
            event_type="process.waiting",
            payload={"process_id": process_id},
        )
        return await self.backend.transition_process(
            run_id=run_id,
            process_id=process_id,
            status=CarrierProcessStatus.waiting,
            command=command,
            events=[event],
            output=output,
        )

    async def save_gate(
        self,
        gate: Gate,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Gate, CommandSubmission]:
        command = RuntimeCommand(
            run_id=gate.run_id,
            command_type="gate.save",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={"gate_id": gate.id, "status": gate.status.value},
        )
        event = RuntimeEvent(
            run_id=gate.run_id,
            carrier_id=gate.carrier_id,
            event_type="gate.saved",
            payload={"gate_id": gate.id, "status": gate.status.value},
        )
        submission = await self.backend.save_gate(gate, command, events=[event])
        if submission.replayed:
            existing_gate_id = submission.command.payload.get("gate_id", gate.id)
            existing = await self.backend.get_gate(
                run_id=gate.run_id,
                gate_id=str(existing_gate_id),
            )
            if existing is None:
                raise ValueError(
                    "Replayed gate command has no stored gate: "
                    f"{existing_gate_id!r}"
                )
            return existing, submission

        return gate, submission

    async def open_gate(
        self,
        gate: Gate,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Gate, CommandSubmission]:
        if gate.status != GateStatus.open:
            raise ValueError("open_gate requires gate status 'open'")
        existing = await self.backend.get_gate(run_id=gate.run_id, gate_id=gate.id)
        replay = await self._replayed_submission(
            run_id=gate.run_id,
            idempotency_key=idempotency_key,
        )
        if replay is not None:
            if existing is None:
                raise ValueError(f"Replayed gate open has no stored gate: {gate.id!r}")
            return existing, replay
        if existing is not None:
            raise ValueError(f"Gate already exists: {gate.id!r}")
        command = RuntimeCommand(
            run_id=gate.run_id,
            command_type="gate.open",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={"gate_id": gate.id, "kind": gate.kind},
        )
        event = RuntimeEvent(
            run_id=gate.run_id,
            carrier_id=gate.carrier_id,
            event_type="gate.opened",
            payload={"gate_id": gate.id, "kind": gate.kind},
        )
        submission = await self.backend.save_gate(gate, command, events=[event])
        if submission.replayed:
            existing_gate_id = submission.command.payload.get("gate_id", gate.id)
            existing = await self.backend.get_gate(
                run_id=gate.run_id,
                gate_id=str(existing_gate_id),
            )
            if existing is None:
                raise ValueError(
                    "Replayed gate open command has no stored gate: "
                    f"{existing_gate_id!r}"
                )
            return existing, submission

        return gate, submission

    async def complete_gate(
        self,
        *,
        run_id: str,
        gate_id: str,
        values: dict[str, Any] | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Gate, CommandSubmission]:
        return await self._finish_gate(
            run_id=run_id,
            gate_id=gate_id,
            values=values,
            status=GateStatus.completed,
            command_type="gate.complete",
            event_type="gate.completed",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def cancel_gate(
        self,
        *,
        run_id: str,
        gate_id: str,
        values: dict[str, Any] | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Gate, CommandSubmission]:
        return await self._finish_gate(
            run_id=run_id,
            gate_id=gate_id,
            values=values,
            status=GateStatus.cancelled,
            command_type="gate.cancel",
            event_type="gate.cancelled",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def expire_gate(
        self,
        *,
        run_id: str,
        gate_id: str,
        values: dict[str, Any] | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Gate, CommandSubmission]:
        return await self._finish_gate(
            run_id=run_id,
            gate_id=gate_id,
            values=values,
            status=GateStatus.expired,
            command_type="gate.expire",
            event_type="gate.expired",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def _finish_gate(
        self,
        *,
        run_id: str,
        gate_id: str,
        values: dict[str, Any] | None,
        status: GateStatus,
        command_type: str,
        event_type: str,
        idempotency_key: str,
        actor: str | None,
        correlation_id: str | None,
        causation_id: str | None,
    ) -> tuple[Gate, CommandSubmission]:
        existing = await self.backend.get_gate(run_id=run_id, gate_id=gate_id)
        if existing is None:
            raise ValueError(f"Unknown gate: {gate_id!r}")
        replay = await self._replayed_submission(
            run_id=run_id,
            idempotency_key=idempotency_key,
        )
        if replay is not None:
            return existing, replay
        if existing.status != GateStatus.open:
            raise ValueError(f"Gate {gate_id!r} is not open: {existing.status.value}")
        command = RuntimeCommand(
            run_id=run_id,
            command_type=command_type,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={
                "gate_id": gate_id,
                "status": status.value,
                "value_keys": sorted((values or {}).keys()),
            },
        )
        event = RuntimeEvent(
            run_id=run_id,
            carrier_id=existing.carrier_id,
            event_type=event_type,
            payload={
                "gate_id": gate_id,
                "status": status.value,
                "value_keys": sorted((values or {}).keys()),
            },
        )
        return await self.backend.transition_gate(
            run_id=run_id,
            gate_id=gate_id,
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
            carrier_id=delivery.carrier.id,
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
        local_carrier = delivery.carrier.model_copy(
            update={
                "run_id": delivery.target.run_id,
                "metadata": {
                    **delivery.carrier.metadata,
                    "source_runtime_id": delivery.source.runtime.id,
                    "source_run_id": delivery.source.run_id,
                    "source_carrier_id": delivery.carrier.id,
                },
                "updated_at": _now(),
            }
        )
        imported = delivery.model_copy(
            update={
                "run_id": delivery.target.run_id,
                "idempotency_key": delivery_key,
                "carrier": local_carrier,
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
            carrier_id=imported.carrier.id,
            event_type="bridge.inbox.imported",
            payload=_bridge_event_payload(imported),
        )
        submission = await self.backend.import_inbox_delivery(
            imported,
            local_carrier,
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
            carrier_id=delivery.carrier.id,
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

    async def list_observations(
        self,
        *,
        run_id: str,
        carrier_id: str | None = None,
    ) -> list[Observation]:
        return await self.backend.list_observations(
            run_id=run_id,
            carrier_id=carrier_id,
        )

    async def list_artifacts(
        self,
        *,
        run_id: str,
        carrier_id: str | None = None,
        kind: str | None = None,
    ) -> list[Artifact]:
        return await self.backend.list_artifacts(
            run_id=run_id,
            carrier_id=carrier_id,
            kind=kind,
        )

    async def list_runs(
        self,
        *,
        status: CarrierRunStatus | None = None,
        limit: int | None = None,
    ) -> list[Run]:
        return await self.backend.list_runs(status=status, limit=limit)

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
        status: CarrierProcessStatus | None = None,
        carrier_id: str | None = None,
    ) -> list[Process]:
        return await self.backend.list_processes(
            run_id=run_id,
            status=status,
            carrier_id=carrier_id,
        )

    async def list_carriers(
        self,
        *,
        run_id: str,
        carrier_type: str | None = None,
        limit: int | None = None,
    ) -> list[Carrier]:
        return await self.backend.list_carriers(
            run_id=run_id,
            carrier_type=carrier_type,
            limit=limit,
        )

    async def list_carrier_types(self, *, run_id: str) -> list[CarrierType]:
        return await self.backend.list_carrier_types(run_id=run_id)

    async def list_carrier_relations(
        self,
        *,
        run_id: str,
        carrier_id: str | None = None,
        relation_type: str | None = None,
    ) -> list[CarrierRelation]:
        return await self.backend.list_carrier_relations(
            run_id=run_id,
            carrier_id=carrier_id,
            relation_type=relation_type,
        )

    async def list_gates(
        self,
        *,
        run_id: str,
        carrier_id: str | None = None,
        status: GateStatus | None = None,
    ) -> list[Gate]:
        return await self.backend.list_gates(
            run_id=run_id,
            carrier_id=carrier_id,
            status=status,
        )

    async def diagnose_waits(
        self,
        *,
        run_id: str,
        carrier_id: str | None = None,
    ) -> CarrierWaitGraphDiagnostic:
        processes = await self.backend.list_processes(
            run_id=run_id,
            carrier_id=carrier_id,
        )
        gates = await self.backend.list_gates(run_id=run_id, carrier_id=carrier_id)
        return _diagnose_carrier_waits(
            run_id=run_id,
            carrier_id=carrier_id,
            processes=processes,
            gates=gates,
        )

    async def list_projections(self, *, run_id: str) -> list[Projection]:
        return await self.backend.list_projections(run_id=run_id)

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
        _dumps(delivery.carrier.model_dump(mode="json")),
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
        "carrier_id": delivery.carrier.id,
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
    if budget.runtime_hops and budget.runtime_hops < 1:
        raise FalaBudgetExceeded(
            "Bridge delivery exceeded runtime hop budget",
            details={"delivery_id": delivery.id, "runtime_hops": budget.runtime_hops},
        )
    if budget.carrier_count and budget.carrier_count < 1:
        raise FalaBudgetExceeded(
            "Bridge delivery exceeded carrier budget",
            details={"delivery_id": delivery.id, "carrier_count": budget.carrier_count},
        )
    if budget.attempts and next_attempt and delivery.attempts + 1 > budget.attempts:
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
            "runtime_hops": budget.runtime_hops - 1
            if budget.runtime_hops > 0
            else 0,
            "carrier_count": budget.carrier_count - 1
            if budget.carrier_count > 0
            else 0,
        }
    )


def _validate_run_status_transition(
    current: CarrierRunStatus,
    target: CarrierRunStatus,
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
            package_digest, flow_id, flow_digest, runtime_version,
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


def _insert_carrier_type_row(
    connection: sqlite3.Connection,
    carrier_type: CarrierType,
) -> None:
    connection.execute(
        """
        INSERT INTO carrier_types (
            run_id, id, title, description, media_types,
            value_schema_json, metadata, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            carrier_type.run_id,
            carrier_type.id,
            carrier_type.title,
            carrier_type.description,
            json.dumps(carrier_type.media_types),
            _dumps(carrier_type.value_schema),
            _dumps(carrier_type.metadata),
            carrier_type.created_at.isoformat(),
            carrier_type.updated_at.isoformat(),
        ),
    )


def _insert_carrier_row(connection: sqlite3.Connection, carrier: Carrier) -> None:
    connection.execute(
        """
        INSERT INTO carriers (
            run_id, id, carrier_type, payload, metadata,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            carrier.run_id,
            carrier.id,
            carrier.carrier_type,
            _dumps(carrier.payload),
            _dumps(carrier.metadata),
            carrier.created_at.isoformat(),
            carrier.updated_at.isoformat(),
        ),
    )


def _upsert_carrier_row(connection: sqlite3.Connection, carrier: Carrier) -> None:
    connection.execute(
        """
        INSERT INTO carriers (
            run_id, id, carrier_type, payload, metadata,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id, id) DO UPDATE SET
            carrier_type = excluded.carrier_type,
            payload = excluded.payload,
            metadata = excluded.metadata,
            updated_at = excluded.updated_at
        """,
        (
            carrier.run_id,
            carrier.id,
            carrier.carrier_type,
            _dumps(carrier.payload),
            _dumps(carrier.metadata),
            carrier.created_at.isoformat(),
            carrier.updated_at.isoformat(),
        ),
    )


def _insert_carrier_relation_row(
    connection: sqlite3.Connection,
    relation: CarrierRelation,
) -> None:
    connection.execute(
        """
        INSERT INTO carrier_relations (
            run_id, id, relation_type, source_carrier_id,
            target_carrier_id, metadata, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            relation.run_id,
            relation.id,
            relation.relation_type,
            relation.source_carrier_id,
            relation.target_carrier_id,
            _dumps(relation.metadata),
            relation.created_at.isoformat(),
        ),
    )


def _insert_observation_row(
    connection: sqlite3.Connection,
    observation: Observation,
) -> None:
    connection.execute(
        """
        INSERT INTO observations (
            run_id, id, kind, carrier_id, values_json,
            metadata, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            observation.run_id,
            observation.id,
            observation.kind,
            observation.carrier_id,
            _dumps(observation.values),
            _dumps(observation.metadata),
            observation.created_at.isoformat(),
        ),
    )


def _insert_artifact_row(
    connection: sqlite3.Connection,
    artifact: Artifact,
) -> None:
    connection.execute(
        """
        INSERT INTO artifacts (
            run_id, id, kind, uri, carrier_id, media_type,
            size_bytes, content_hash, metadata, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            artifact.run_id,
            artifact.id,
            artifact.kind,
            artifact.uri,
            artifact.carrier_id,
            artifact.media_type,
            artifact.size_bytes,
            artifact.content_hash,
            _dumps(artifact.metadata),
            artifact.created_at.isoformat(),
        ),
    )


def _insert_process_row(connection: sqlite3.Connection, process: Process) -> None:
    connection.execute(
        """
        INSERT INTO processes (
            run_id, id, process_type, carrier_id, status, priority,
            attempt, max_attempts, available_at, lease_owner,
            lease_expires_at, input_json, output_json, error_json,
            metadata, created_at, updated_at, started_at, finished_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        _process_args(process),
    )


def _insert_gate_row(connection: sqlite3.Connection, gate: Gate) -> None:
    connection.execute(
        """
        INSERT INTO gates (
            run_id, id, kind, carrier_id, status,
            values_json, metadata, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            gate.run_id,
            gate.id,
            gate.kind,
            gate.carrier_id,
            gate.status.value,
            _dumps(gate.values),
            _dumps(gate.metadata),
            gate.created_at.isoformat(),
            gate.updated_at.isoformat(),
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
            carrier_json, event_ref, pool_id, budget, status,
            attempts, metadata, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id, id) DO UPDATE SET
            idempotency_key = excluded.idempotency_key,
            source_ref = excluded.source_ref,
            target_ref = excluded.target_ref,
            carrier_json = excluded.carrier_json,
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
                run_id, sequence, id, event_type, carrier_id,
                process_id, schema_version, command_id, actor,
                correlation_id, causation_id, payload, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                stored_event.run_id,
                stored_event.sequence,
                stored_event.id,
                stored_event.event_type,
                stored_event.carrier_id,
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
        "artifact_bytes": _int_scalar(
            connection,
            "SELECT COALESCE(SUM(size_bytes), 0) FROM artifacts WHERE run_id = ?",
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
        "spawned_run_count": 0,
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
        "artifact_count": _count_rows(connection, "artifacts", run_id),
        "carrier_count": _count_rows(connection, "carriers", run_id),
        "carrier_type_counts": _group_counts(
            connection,
            table="carriers",
            column="carrier_type",
            run_id=run_id,
        ),
        "event_count": sum(event_type_counts.values()),
        "event_type_counts": event_type_counts,
        "gate_count": _count_rows(connection, "gates", run_id),
        "gate_status_counts": _group_counts(
            connection,
            table="gates",
            column="status",
            run_id=run_id,
        ),
        "observation_count": _count_rows(connection, "observations", run_id),
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


def _diagnose_carrier_waits(
    *,
    run_id: str,
    carrier_id: str | None,
    processes: Sequence[Process],
    gates: Sequence[Gate],
) -> CarrierWaitGraphDiagnostic:
    processes_by_id = {process.id: process for process in processes}
    gates_by_id = {gate.id: gate for gate in gates}
    buckets: dict[str, list[str]] = {
        status.value: [] for status in CarrierProcessStatus
    }
    for process in sorted(processes, key=lambda item: item.id):
        buckets[process.status.value].append(process.id)

    open_gates = sorted(
        gate.id for gate in gates if gate.status == GateStatus.open
    )
    wait_edges: dict[str, list[str]] = {}
    blocked: list[CarrierWaitDiagnosticIssue] = []

    for process in sorted(processes, key=lambda item: item.id):
        if process.status == CarrierProcessStatus.retry_wait:
            blocked.append(
                CarrierWaitDiagnosticIssue(
                    process_id=process.id,
                    status=process.status,
                    reason="retry_wait",
                    data={"available_at": process.available_at.isoformat()},
                )
            )
            continue
        if process.status != CarrierProcessStatus.waiting:
            continue

        process_dependencies = _carrier_wait_refs(
            process,
            "wait_for_processes",
            "wait_for_process_ids",
            "blocked_by_processes",
        )
        gate_dependencies = _carrier_wait_refs(
            process,
            "wait_for_gates",
            "wait_for_gate_ids",
            "blocked_by_gates",
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
            if dependency is None or dependency.status != CarrierProcessStatus.succeeded:
                blocked_by.append(dependency_id)
                process_edges.append(dependency_id)

        for gate_id in gate_dependencies:
            gate = gates_by_id.get(gate_id)
            gate_status = gate.status.value if gate is not None else None
            key = f"gate:{gate_id}"
            dependency_statuses[key] = gate_status
            if gate is None or gate.status != GateStatus.completed:
                blocked_by.append(key)

        if process_edges:
            wait_edges[process.id] = process_edges
        blocked.append(
            CarrierWaitDiagnosticIssue(
                process_id=process.id,
                status=process.status,
                reason="waiting"
                if blocked_by
                else "waiting_without_known_blocker",
                blocked_by=blocked_by,
                dependency_statuses=dependency_statuses,
            )
        )

    deadlocks = _carrier_wait_cycles(wait_edges)
    return CarrierWaitGraphDiagnostic(
        run_id=run_id,
        carrier_id=carrier_id,
        deadlocked=bool(deadlocks),
        deadlocks=deadlocks,
        wait_edges=wait_edges,
        blocked=blocked,
        open_gates=open_gates,
        pending=buckets[CarrierProcessStatus.pending.value],
        ready=buckets[CarrierProcessStatus.ready.value],
        running=buckets[CarrierProcessStatus.running.value],
        waiting=buckets[CarrierProcessStatus.waiting.value],
        retry_wait=buckets[CarrierProcessStatus.retry_wait.value],
        succeeded=buckets[CarrierProcessStatus.succeeded.value],
        failed=buckets[CarrierProcessStatus.failed.value],
        cancel_requested=buckets[CarrierProcessStatus.cancel_requested.value],
        cancelled=buckets[CarrierProcessStatus.cancelled.value],
        timed_out=buckets[CarrierProcessStatus.timed_out.value],
    )


def _carrier_wait_refs(process: Process, *keys: str) -> list[str]:
    values: list[str] = []
    for source in (process.input, process.metadata):
        for key in keys:
            value = source.get(key)
            if isinstance(value, str):
                values.append(value)
            elif isinstance(value, list):
                values.extend(item for item in value if isinstance(item, str))
    return list(dict.fromkeys(values))


def _carrier_wait_cycles(edges: dict[str, list[str]]) -> list[list[str]]:
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
        run.flow_id,
        run.flow_digest,
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
        status=CarrierRunStatus(row["status"]),
        title=row["title"],
        package_id=row["package_id"],
        package_version=row["package_version"],
        package_digest=row["package_digest"],
        flow_id=row["flow_id"],
        flow_digest=row["flow_digest"],
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
        json.dumps(pool.carrier_types, sort_keys=True, separators=(",", ":")),
        _dumps(pool.metadata),
    )


def _runtime_pool_from_row(row: sqlite3.Row) -> RuntimePool:
    return RuntimePool(
        id=row["id"],
        runtimes=_loads_runtime_refs(row["runtimes_json"]),
        carrier_types=_loads_str_list(row["carrier_types"]),
        metadata=_loads(row["metadata"]),
    )


def _delegation_policy_args(policy: DelegationPolicy) -> tuple[Any, ...]:
    return (
        policy.id,
        policy.pool_id,
        json.dumps(policy.carrier_types, sort_keys=True, separators=(",", ":")),
        _dumps(policy.budget.model_dump(mode="json")),
        _dumps(policy.metadata),
    )


def _delegation_policy_from_row(row: sqlite3.Row) -> DelegationPolicy:
    return DelegationPolicy(
        id=row["id"],
        pool_id=row["pool_id"],
        carrier_types=_loads_str_list(row["carrier_types"]),
        budget=RuntimeBudget.model_validate(_loads(row["budget"])),
        metadata=_loads(row["metadata"]),
    )


def _process_args(process: Process) -> tuple[Any, ...]:
    return (
        process.run_id,
        process.id,
        process.process_type,
        process.carrier_id,
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
        carrier_id=row["carrier_id"],
        status=CarrierProcessStatus(row["status"]),
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
        created_at=_dt(row["created_at"]),
        updated_at=_dt(row["updated_at"]),
        started_at=_dt(row["started_at"]) if row["started_at"] is not None else None,
        finished_at=_dt(row["finished_at"]) if row["finished_at"] is not None else None,
    )


def _carrier_from_row(row: sqlite3.Row) -> Carrier:
    return Carrier(
        id=row["id"],
        run_id=row["run_id"],
        carrier_type=row["carrier_type"],
        payload=_loads(row["payload"]),
        metadata=_loads(row["metadata"]),
        created_at=_dt(row["created_at"]),
        updated_at=_dt(row["updated_at"]),
    )


def _carrier_type_from_row(row: sqlite3.Row) -> CarrierType:
    return CarrierType(
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


def _carrier_relation_from_row(row: sqlite3.Row) -> CarrierRelation:
    return CarrierRelation(
        id=row["id"],
        run_id=row["run_id"],
        relation_type=row["relation_type"],
        source_carrier_id=row["source_carrier_id"],
        target_carrier_id=row["target_carrier_id"],
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
        carrier_id=row["carrier_id"],
        process_id=row["process_id"],
        sequence=row["sequence"],
        command_id=row["command_id"],
        actor=row["actor"],
        correlation_id=row["correlation_id"],
        causation_id=row["causation_id"],
        payload=_loads(row["payload"]),
        created_at=_dt(row["created_at"]),
    )


def _observation_from_row(row: sqlite3.Row) -> Observation:
    return Observation(
        id=row["id"],
        run_id=row["run_id"],
        kind=row["kind"],
        carrier_id=row["carrier_id"],
        values=_loads(row["values_json"]),
        metadata=_loads(row["metadata"]),
        created_at=_dt(row["created_at"]),
    )


def _artifact_from_row(row: sqlite3.Row) -> Artifact:
    return Artifact(
        id=row["id"],
        run_id=row["run_id"],
        kind=row["kind"],
        uri=row["uri"],
        carrier_id=row["carrier_id"],
        media_type=row["media_type"],
        size_bytes=row["size_bytes"],
        content_hash=row["content_hash"],
        metadata=_loads(row["metadata"]),
        created_at=_dt(row["created_at"]),
    )


def _gate_from_row(row: sqlite3.Row) -> Gate:
    return Gate(
        id=row["id"],
        run_id=row["run_id"],
        kind=row["kind"],
        carrier_id=row["carrier_id"],
        status=GateStatus(row["status"]),
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
        carrier=Carrier.model_validate(_loads(row["carrier_json"])),
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


__all__ = [
    "Artifact",
    "BridgeDelivery",
    "BridgeDeliveryStatus",
    "CarrierProcessStatus",
    "Carrier",
    "CarrierRunStatus",
    "CarrierRelation",
    "CarrierType",
    "CarrierWaitDiagnosticIssue",
    "CarrierWaitGraphDiagnostic",
    "CommandSubmission",
    "DelegationPolicy",
    "EventRef",
    "Gate",
    "GateStatus",
    "Observation",
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
    "RunRef",
    "SQLITE_RUNTIME_SCHEMA_VERSION",
    "SQLiteRuntimeBackend",
]
