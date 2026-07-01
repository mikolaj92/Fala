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


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


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
    carrier_id: str | None = None
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
    async def put_run(self, run: Run) -> None: ...

    async def get_run(self, *, run_id: str) -> Run | None: ...

    async def list_runs(
        self,
        *,
        status: CarrierRunStatus | None = None,
        limit: int | None = None,
    ) -> list[Run]: ...

    async def put_carrier_type(self, carrier_type: CarrierType) -> None: ...

    async def get_carrier_type(
        self, *, run_id: str, carrier_type_id: str
    ) -> CarrierType | None: ...

    async def list_carrier_types(self, *, run_id: str) -> list[CarrierType]: ...

    async def put_carrier(self, carrier: Carrier) -> None: ...

    async def get_carrier(self, *, run_id: str, carrier_id: str) -> Carrier | None: ...

    async def list_carriers(
        self,
        *,
        run_id: str,
        carrier_type: str | None = None,
        limit: int | None = None,
    ) -> list[Carrier]: ...

    async def put_carrier_relation(self, relation: CarrierRelation) -> None: ...

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

    async def list_events(
        self,
        *,
        run_id: str,
        carrier_id: str | None = None,
        after_sequence: int | None = None,
        limit: int | None = None,
    ) -> list[RuntimeEvent]: ...

    async def put_observation(self, observation: Observation) -> None: ...

    async def list_observations(
        self, *, run_id: str, carrier_id: str | None = None
    ) -> list[Observation]: ...

    async def put_artifact(self, artifact: Artifact) -> None: ...

    async def get_artifact(self, *, run_id: str, artifact_id: str) -> Artifact | None: ...

    async def list_artifacts(
        self,
        *,
        run_id: str,
        carrier_id: str | None = None,
        kind: str | None = None,
    ) -> list[Artifact]: ...

    async def put_process(self, process: Process) -> None: ...

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

    async def put_gate(self, gate: Gate) -> None: ...

    async def get_gate(self, *, run_id: str, gate_id: str) -> Gate | None: ...

    async def complete_gate(
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

    async def get_projection(self, *, run_id: str, name: str) -> Projection | None: ...

    async def list_projections(self, *, run_id: str) -> list[Projection]: ...

    async def rebuild_projections(
        self,
        *,
        run_id: str,
        names: Sequence[str] | None = None,
    ) -> list[Projection]: ...

    async def put_outbox_delivery(self, delivery: BridgeDelivery) -> None: ...

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
_SQLITE_SCHEMA_VERSION = 1
_TERMINAL_RUN_STATUSES = {
    CarrierRunStatus.completed,
    CarrierRunStatus.failed,
    CarrierRunStatus.cancelled,
    CarrierRunStatus.timed_out,
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
                    carrier_id TEXT,
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

    async def put_carrier_type(self, carrier_type: CarrierType) -> None:
        async with self._lock:
            with self._connect() as connection:
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
                            command_id, actor, correlation_id, causation_id,
                            payload, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            stored_event.run_id,
                            stored_event.sequence,
                            stored_event.id,
                            stored_event.event_type,
                            stored_event.carrier_id,
                            stored_event.command_id,
                            stored_event.actor,
                            stored_event.correlation_id,
                            stored_event.causation_id,
                            _dumps(stored_event.payload),
                            stored_event.created_at.isoformat(),
                        ),
                    )
                    stored_events.append(stored_event)

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

    async def get_gate(self, *, run_id: str, gate_id: str) -> Gate | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM gates WHERE run_id = ? AND id = ?",
                (run_id, gate_id),
            ).fetchone()
        return _gate_from_row(row) if row is not None else None

    async def complete_gate(
        self,
        *,
        run_id: str,
        gate_id: str,
        values: dict[str, Any] | None = None,
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
                        GateStatus.completed.value,
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
                connection.commit()

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
            list(dict.fromkeys(names)) if names is not None else list(_BUILT_IN_PROJECTIONS)
        )
        unsupported = sorted(set(requested) - set(_BUILT_IN_PROJECTIONS))
        if unsupported:
            raise ValueError(f"Unknown projection rebuild name: {unsupported[0]}")

        rebuilt: list[Projection] = []
        async with self._lock:
            with self._connect() as connection:
                for name in requested:
                    projection = _build_run_summary_projection(connection, run_id)
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
                    rebuilt.append(projection)
                connection.commit()
        return rebuilt

    async def put_outbox_delivery(self, delivery: BridgeDelivery) -> None:
        await self._put_bridge_delivery("bridge_outbox", delivery)

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
                connection.commit()

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
        submission = await self.backend.submit_command(command, events=[event])
        if submission.replayed:
            existing_run_id = submission.command.payload.get("run_id", run.id)
            existing = await self.backend.get_run(run_id=str(existing_run_id))
            if existing is None:
                raise ValueError(
                    "Replayed run create command has no stored run: "
                    f"{existing_run_id!r}"
                )
            return existing, submission

        await self.backend.put_run(run)
        return run, submission

    async def set_run_status(
        self,
        *,
        run_id: str,
        status: CarrierRunStatus,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Run, CommandSubmission]:
        existing = await self.backend.get_run(run_id=run_id)
        if existing is None:
            raise ValueError(f"Unknown run: {run_id!r}")
        if existing.status != status:
            _validate_run_status_transition(existing.status, status)
        command = RuntimeCommand(
            run_id=run_id,
            command_type="run.status.set",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={"run_id": run_id, "status": status.value},
        )
        event = RuntimeEvent(
            run_id=run_id,
            event_type="run.status.changed",
            payload={"from": existing.status.value, "to": status.value},
        )
        submission = await self.backend.submit_command(command, events=[event])
        if submission.replayed:
            replayed_run = await self.backend.get_run(run_id=run_id)
            if replayed_run is None:
                raise ValueError(f"Replayed run status command has no stored run: {run_id!r}")
            return replayed_run, submission

        now = _now()
        updated = existing.model_copy(
            update={
                "status": status,
                "updated_at": now,
                "started_at": existing.started_at
                or (now if status == CarrierRunStatus.active else None),
                "finished_at": now
                if status in _TERMINAL_RUN_STATUSES
                else existing.finished_at,
            }
        )
        await self.backend.put_run(updated)
        return updated, submission

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
        submission = await self.backend.submit_command(command, events=[event])
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

        await self.backend.put_carrier_type(carrier_type)
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
        submission = await self.backend.submit_command(command, events=[event])
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

        await self.backend.put_carrier(carrier)
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
        submission = await self.backend.submit_command(command, events=[event])
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

        await self.backend.put_carrier_relation(relation)
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
        submission = await self.backend.submit_command(command, events=[event])
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

        await self.backend.put_observation(observation)
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
        submission = await self.backend.submit_command(command, events=[event])
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

        await self.backend.put_artifact(artifact)
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
            event_type="process.scheduled",
            payload={
                "process_id": process.id,
                "process_type": process.process_type,
                "status": process.status.value,
            },
        )
        submission = await self.backend.submit_command(command, events=[event])
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

        await self.backend.put_process(process)
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
            event_type="process.completed",
            payload={"process_id": process_id},
        )
        submission = await self.backend.submit_command(command, events=[event])
        if submission.replayed:
            existing = await self.backend.get_process(run_id=run_id, process_id=process_id)
            if existing is None:
                raise ValueError(f"Replayed process complete has no stored process: {process_id!r}")
            return existing, submission
        return (
            await self.backend.complete_process(
                run_id=run_id,
                process_id=process_id,
                output=output,
            ),
            submission,
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
            event_type="process.failed",
            payload={"process_id": process_id},
        )
        submission = await self.backend.submit_command(command, events=[event])
        if submission.replayed:
            existing = await self.backend.get_process(run_id=run_id, process_id=process_id)
            if existing is None:
                raise ValueError(f"Replayed process fail has no stored process: {process_id!r}")
            return existing, submission
        return (
            await self.backend.fail_process(
                run_id=run_id,
                process_id=process_id,
                error=error,
            ),
            submission,
        )

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
            event_type="process.retry_scheduled",
            payload={"process_id": process_id},
        )
        submission = await self.backend.submit_command(command, events=[event])
        if submission.replayed:
            existing = await self.backend.get_process(run_id=run_id, process_id=process_id)
            if existing is None:
                raise ValueError(f"Replayed process retry has no stored process: {process_id!r}")
            return existing, submission
        return (
            await self.backend.retry_process(
                run_id=run_id,
                process_id=process_id,
                available_at=available_at,
                error=error,
            ),
            submission,
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
        submission = await self.backend.submit_command(command, events=[event])
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

        await self.backend.put_gate(gate)
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
        existing = await self.backend.get_gate(run_id=run_id, gate_id=gate_id)
        if existing is None:
            raise ValueError(f"Unknown gate: {gate_id!r}")
        command = RuntimeCommand(
            run_id=run_id,
            command_type="gate.complete",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={
                "gate_id": gate_id,
                "status": GateStatus.completed.value,
                "value_keys": sorted((values or {}).keys()),
            },
        )
        event = RuntimeEvent(
            run_id=run_id,
            carrier_id=existing.carrier_id,
            event_type="gate.completed",
            payload={
                "gate_id": gate_id,
                "status": GateStatus.completed.value,
                "value_keys": sorted((values or {}).keys()),
            },
        )
        submission = await self.backend.submit_command(command, events=[event])
        if submission.replayed:
            replayed_gate = await self.backend.get_gate(run_id=run_id, gate_id=gate_id)
            if replayed_gate is None:
                raise ValueError(
                    f"Replayed gate completion has no stored gate: {gate_id!r}"
                )
            return replayed_gate, submission

        return (
            await self.backend.complete_gate(
                run_id=run_id,
                gate_id=gate_id,
                values=values,
            ),
            submission,
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
        submission = await self.backend.submit_command(command, events=[event])
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

        await self.backend.put_projection(projection)
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
        submission = await self.backend.submit_command(command, events=[event])
        if submission.replayed:
            projections: list[Projection] = []
            for name in requested:
                existing = await self.backend.get_projection(run_id=run_id, name=name)
                if existing is None:
                    raise ValueError(
                        "Replayed projection rebuild has no stored projection: "
                        f"{name!r}"
                    )
                projections.append(existing)
            return projections, submission

        return (
            await self.backend.rebuild_projections(run_id=run_id, names=requested),
            submission,
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
        submission = await self.backend.submit_command(command, events=[event])
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

        await self.backend.put_outbox_delivery(delivery)
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
        submission = await self.backend.submit_command(command, events=[event])
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

        await self.backend.put_carrier(local_carrier)
        await self.backend.put_inbox_delivery(imported)
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
        delivery_submission = await self.backend.submit_command(command, events=[event])
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

        await self.backend.put_outbox_delivery(delivered)
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
        carrier_id=row["carrier_id"],
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
    "SQLiteRuntimeBackend",
]
