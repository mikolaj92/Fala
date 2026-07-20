"""Journal port for Fala event-stream core.

Types and Protocol only in this package for PR1. Sink implementations
(InMemory, Sqlite, Jsonl) arrive in later PRs — see docs/EVENT_STREAM_CORE.md.
"""

from __future__ import annotations

from fala.journal.memory import InMemoryJournal, apply_facts
from fala.journal.protocol import Journal
from fala.journal.types import (
    KNOWN_STATE_FACT_ENTITIES,
    AppendResult,
    ClaimRequest,
    ClaimResult,
    CommandUnit,
    JournalBatch,
    StateFact,
    StateFactOp,
    leading_command,
    leading_idempotency_key,
)

__all__ = [
    "KNOWN_STATE_FACT_ENTITIES",
    "AppendResult",
    "ClaimRequest",
    "ClaimResult",
    "CommandUnit",
    "InMemoryJournal",
    "Journal",
    "JournalBatch",
    "StateFact",
    "StateFactOp",
    "apply_facts",
    "leading_command",
    "leading_idempotency_key",
]
