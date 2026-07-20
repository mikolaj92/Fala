"""TeeJournal — fan-out appends to primary + secondaries (same land adapters)."""

from std.collections import List

from fala.journal_port import (
    AppendResult,
    ClaimRequest,
    ClaimResult,
    JournalBatch,
)
from fala.memory_journal import InMemoryJournal
from fala.processes import ProcessRecord


struct TeeJournal(Movable):
    """Primary is authoritative for claim/list; secondaries mirror appends."""

    var primary: InMemoryJournal
    var secondary: InMemoryJournal

    def __init__(
        out self,
        primary_stream: String = "memory://tee-primary",
        secondary_stream: String = "memory://tee-secondary",
    ):
        self.primary = InMemoryJournal(primary_stream)
        self.secondary = InMemoryJournal(secondary_stream)

    def runtime_uri(self) -> String:
        return "tee://" + self.primary.runtime_uri()

    def seed_process(mut self, process: ProcessRecord):
        self.primary.seed_process(process)
        self.secondary.seed_process(process)

    def append_batch(mut self, batch: JournalBatch) raises -> AppendResult:
        var result = self.primary.append_batch(batch)
        if not result.replayed:
            _ = self.secondary.append_batch(result.batch.copy())
        return result^

    def claim_next(mut self, request: ClaimRequest) raises -> ClaimResult:
        var result = self.primary.claim_next(request)
        if result.has_batch and not result.replayed:
            _ = self.secondary.append_batch(result.batch.copy())
        return result^

    def list_events_primary(mut self, run_id: String) raises:
        return self.primary.list_events(run_id)

    def list_events_secondary(mut self, run_id: String) raises:
        return self.secondary.list_events(run_id)

    def secondary_event_count(mut self, run_id: String) raises -> Int:
        return len(self.secondary.list_events(run_id))

    def primary_event_count(mut self, run_id: String) raises -> Int:
        return len(self.primary.list_events(run_id))
