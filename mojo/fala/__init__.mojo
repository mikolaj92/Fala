"""Native Fala core package (event-first, no SQLite dependency).

Production Mojo boundary for:
- pure lifecycle policy (status, processes)
- correlation planning (correlation)
- JournalPort types + InMemoryJournal
- shared models / JSON / validation

SQLite and other sinks live as adapters in later phases — see
docs/MOJO_EVENT_STREAM_MIGRATION.md.
"""

from .status import (
    ProcessStatus,
    RunStatus,
    can_transition_process,
    can_transition_run,
    can_replay_terminal_process,
)
from .processes import (
    PROCESS_SCHEMA_VERSION,
    ProcessRecord,
    process_is_claimable,
    ready_processes,
    claim_process,
    actor_can_transition,
    transition_process,
    retry_is_eligible,
    retry_backoff_seconds,
    retry_process,
    expire_process,
)
from .journal_port import (
    StateFact,
    CommandRecord,
    EventRecord,
    CommandUnit,
    JournalBatch,
    AppendResult,
    ClaimRequest,
    ClaimResult,
    leading_command,
    leading_idempotency_key,
)
from .memory_journal import InMemoryJournal
