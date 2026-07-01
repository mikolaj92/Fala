from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import Sequence
from datetime import datetime, timezone
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


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


class Carrier(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: _new_id("carrier"))
    run_id: str
    carrier_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)


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


class RuntimeBackend(Protocol):
    async def put_carrier(self, carrier: Carrier) -> None: ...

    async def get_carrier(self, *, run_id: str, carrier_id: str) -> Carrier | None: ...

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

    async def put_gate(self, gate: Gate) -> None: ...

    async def get_gate(self, *, run_id: str, gate_id: str) -> Gate | None: ...

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

                CREATE INDEX IF NOT EXISTS idx_runtime_events_carrier
                    ON runtime_events (run_id, carrier_id, sequence);
                CREATE INDEX IF NOT EXISTS idx_observations_carrier
                    ON observations (run_id, carrier_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_gates_status
                    ON gates (run_id, status, updated_at);
                """
            )
            connection.commit()

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


class RuntimeBackendService:
    def __init__(self, backend: RuntimeBackend) -> None:
        self.backend = backend

    @classmethod
    def sqlite(cls, path: str | Path) -> "RuntimeBackendService":
        return cls(SQLiteRuntimeBackend(path))

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


__all__ = [
    "Carrier",
    "CommandSubmission",
    "Gate",
    "GateStatus",
    "Observation",
    "Projection",
    "RuntimeBackend",
    "RuntimeBackendService",
    "RuntimeCommand",
    "RuntimeEvent",
    "SQLiteRuntimeBackend",
]
