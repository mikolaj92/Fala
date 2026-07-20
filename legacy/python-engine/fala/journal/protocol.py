"""Journal Protocol — durable command-path port for event-stream core.

Implementations (InMemory, Sqlite wrap, Jsonl) live in later PRs.
See docs/EVENT_STREAM_CORE.md for batch, claim, and replay semantics.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from fala.journal.types import (
    AppendResult,
    ClaimRequest,
    ClaimResult,
    JournalBatch,
)
from fala.runtime_backend import RuntimeCommand, RuntimeEvent


@runtime_checkable
class Journal(Protocol):
    """Durable command-path port. Each sink is the single authority for its data."""

    @property
    def runtime_uri(self) -> str:
        """e.g. sqlite:///abs/path, memory://local, jsonl:///abs/path.journal.jsonl"""
        ...

    async def append_batch(self, batch: JournalBatch) -> AppendResult:
        """Atomically accept the batch or return prior replay.

        Idempotency:
        - Primary key for replay decision: the **first** unit's
          ``(run_id, command.idempotency_key)``.
        - If the leading key exists: return ``replayed=True``, do not mutate;
          primary unit events are empty (parity with Correlator today).
        - InMemory: apply **all** units (sequences, command index, facts) in
          one lock; if a non-leading key exists while leading does not, abort
          (corrupt partial history).
        - SqliteJournal: use **leading unit only** as input to the Correlator
          TX method; non-leading units are ignored as inputs (Correlator
          regenerates side effects). ``AppendResult.units`` may be synthesized
          post-TX from the command/event log.

        Multi-command durability is one atomic commit — same as Correlator TX.
        """
        ...

    async def claim_next(self, request: ClaimRequest) -> ClaimResult:
        """Select + mutate under the sink's atomic section (no TOCTOU).

        Semantics match Correlator.claim_next_ready_process:
        1. Under sink lock / BEGIN IMMEDIATE: reap expired leases (fail units).
        2. Select next claimable process; if none, commit reaps only (if any).
        3. Else claim with lease; append claim unit; commit entire batch.
        4. Return process snapshot after claim, plus the committed batch.

        Core/driver **must not** pre-select a process id then append for
        multi-worker sinks.
        """
        ...

    async def get_command_by_idempotency(
        self, *, run_id: str, idempotency_key: str
    ) -> RuntimeCommand | None:
        """Lookup a previously accepted command by idempotency key."""
        ...

    async def list_events(
        self,
        *,
        run_id: str,
        after_sequence: int | None = None,
        impulse_id: str | None = None,
        limit: int | None = None,
    ) -> list[RuntimeEvent]:
        """Ordered events for a run (after_sequence exclusive if set)."""
        ...

    async def load(
        self,
        *,
        run_id: str | None = None,
        after_journal_seq: int | None = None,
        limit: int | None = None,
    ) -> list[JournalBatch]:
        """Ordered durable history for recovery / export."""
        ...
