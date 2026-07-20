"""Journal port for Fala event-stream core.

See docs/EVENT_STREAM_CORE.md for batch, claim, and JournalBackedBackend.
"""

from __future__ import annotations

from fala.journal.backend import InMemoryRuntimeBackend, JournalBackedBackend
from fala.journal.jsonl import (
    JsonlJournal,
    TeeJournal,
    append_line_durable,
    decode_journal_line,
    encode_journal_line,
    repair_torn_jsonl,
)
from fala.journal.memory import InMemoryJournal, apply_facts
from fala.journal.protocol import Journal
from fala.journal.sqlite import SqliteJournal
from fala.journal.stream import (
    STREAM_MERGED_KIND,
    STREAM_MERGED_VERSION,
    flatten_stream_merged,
    nest_child_batch,
    nest_child_batches,
    stream_merged_envelope,
)
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
    "InMemoryRuntimeBackend",
    "Journal",
    "JournalBackedBackend",
    "JournalBatch",
    "JsonlJournal",
    "STREAM_MERGED_KIND",
    "STREAM_MERGED_VERSION",
    "SqliteJournal",
    "StateFact",
    "StateFactOp",
    "TeeJournal",
    "append_line_durable",
    "apply_facts",
    "decode_journal_line",
    "encode_journal_line",
    "flatten_stream_merged",
    "leading_command",
    "leading_idempotency_key",
    "nest_child_batch",
    "nest_child_batches",
    "repair_torn_jsonl",
    "stream_merged_envelope",
]
