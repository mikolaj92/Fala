"""Journal batch types for the event-stream core durability port.

See docs/EVENT_STREAM_CORE.md. These models are the normative wire shape for
atomic multi-command batches; no sink implementation lives in this module.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from fala.runtime_backend import Process, RuntimeCommand, RuntimeEvent

StateFactOp = Literal["upsert", "delete"]

# Known entity kinds for StateFact.entity (not enforced as a closed enum so
# domain extensions can add kinds without a core release).
KNOWN_STATE_FACT_ENTITIES: frozenset[str] = frozenset(
    {
        "run",
        "impulse",
        "impulse_type",
        "impulse_relation",
        "association",
        "reaction",
        "process",
        "homeostat",
        "projection",
        "bridge_outbox",
        "bridge_inbox",
        "runtime_pool",
        "delegation_policy",
    }
)


class StateFact(BaseModel):
    """Materialized entity change applied in the same atomic batch as its unit.

    ``body`` is a full entity snapshot after the op for ``upsert`` (replace, not
    deep-merge). Keys identify the row. See docs/EVENT_STREAM_CORE.md Recovery.
    """

    model_config = ConfigDict(extra="forbid")

    entity: str
    op: StateFactOp
    key: dict[str, str]
    body: dict[str, Any] = Field(default_factory=dict)


class CommandUnit(BaseModel):
    """One append-only command with its linked events and materialization facts."""

    model_config = ConfigDict(extra="forbid")

    command: RuntimeCommand
    events: list[RuntimeEvent] = Field(default_factory=list)
    facts: list[StateFact] = Field(default_factory=list)


class JournalBatch(BaseModel):
    """Atomic durability unit — may contain N CommandUnits (N >= 1 for append).

    Examples:
    - create_run: 1 unit
    - claim_next result: 0..K fail units + 0..1 claim unit (built inside claim_next)
    - transition_process complete + auto-advance: 1 + M ready/cancel units
    """

    model_config = ConfigDict(extra="forbid")

    journal_seq: int | None = None
    run_id: str
    units: list[CommandUnit] = Field(min_length=1)
    stream_id: str | None = None
    parent_stream_id: str | None = None
    parent_process_id: str | None = None


class AppendResult(BaseModel):
    """Result of Journal.append_batch.

    On ``replayed=True``: event lists on units are empty (parity with Correlator).
    On ``replayed=False``: events carry assigned sequence + command_id.
    """

    model_config = ConfigDict(extra="forbid")

    batch: JournalBatch
    replayed: bool = False
    units: list[CommandUnit] = Field(default_factory=list)


class ClaimRequest(BaseModel):
    """Input to Journal.claim_next (select + mutate under sink atomic section)."""

    model_config = ConfigDict(extra="forbid")

    worker_id: str
    run_id: str | None = None
    lease_seconds: float = 300.0
    all_runs: bool = False


class ClaimResult(BaseModel):
    """Result of Journal.claim_next.

    ``batch`` may be non-None with only fail units when reaps happened but
    nothing was claimed. ``batch`` is None when no process was claimed and
    no reaps ran.
    """

    model_config = ConfigDict(extra="forbid")

    process: Process | None = None
    batch: JournalBatch | None = None
    replayed: bool = False


def leading_command(batch: JournalBatch) -> RuntimeCommand:
    """Return the primary (leading) command used for batch idempotency."""
    return batch.units[0].command


def leading_idempotency_key(batch: JournalBatch) -> tuple[str, str]:
    """``(run_id, idempotency_key)`` for the leading unit."""
    command = leading_command(batch)
    return command.run_id, command.idempotency_key
