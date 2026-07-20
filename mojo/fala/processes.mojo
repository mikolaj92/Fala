"""Pure in-memory process lifecycle helpers.

This module deliberately contains no persistence or Python bridge.  Timestamps are
Unix-like seconds supplied by the caller; an empty lease owner and a zero lease
expiry represent an absent lease.  Persistence adapters can map their own time
and nullable representations to :struct:`ProcessRecord`.
"""

from std.collections import List

from .status import ProcessStatus, can_transition_process


# Kept in sync with the native persistence contract.  This module does not
# perform migrations; adapters may use this value when serializing records.
comptime PROCESS_SCHEMA_VERSION: Int = 6


struct ProcessRecord(Copyable, Movable):
    """A persistence-neutral process row.

    ``attempt`` is the number of claims already made.  A claim increments it
    exactly once.  ``lease_owner == \"\"`` means no owner and
    ``lease_expires_at == 0.0`` means no expiry.
    """

    var id: String
    var run_id: String
    var status: ProcessStatus
    var priority: Int
    var attempt: Int
    var max_attempts: Int
    var available_at: Float64
    var created_at: Float64
    var lease_owner: String
    var lease_expires_at: Float64
    def __copyinit__(mut self, other: Self):
        self.id = other.id
        self.run_id = other.run_id
        self.status = ProcessStatus(other.status.value)
        self.priority = other.priority
        self.attempt = other.attempt
        self.max_attempts = other.max_attempts
        self.available_at = other.available_at
        self.created_at = other.created_at
        self.lease_owner = other.lease_owner
        self.lease_expires_at = other.lease_expires_at
    def __moveinit__(mut self, var other: Self):
        self.id = other.id
        self.run_id = other.run_id
        self.status = ProcessStatus(other.status.value)
        self.priority = other.priority
        self.attempt = other.attempt
        self.max_attempts = other.max_attempts
        self.available_at = other.available_at
        self.created_at = other.created_at
        self.lease_owner = other.lease_owner
        self.lease_expires_at = other.lease_expires_at



    def __init__(
        out self,
        id: String,
        run_id: String,
        status: ProcessStatus = ProcessStatus.pending(),
        priority: Int = 0,
        attempt: Int = 0,
        max_attempts: Int = 1,
        available_at: Float64 = 0.0,
        created_at: Float64 = 0.0,
        lease_owner: String = "",
        lease_expires_at: Float64 = 0.0,
    ):
        self.id = id
        self.run_id = run_id
        self.status = ProcessStatus(status.value)
        self.priority = priority
        self.attempt = attempt
        self.max_attempts = max_attempts
        self.available_at = available_at
        self.created_at = created_at
        self.lease_owner = lease_owner
        self.lease_expires_at = lease_expires_at

    def has_lease(self) -> Bool:
        return self.lease_owner != ""

    def lease_is_expired(self, now: Float64) -> Bool:
        return self.has_lease() and self.lease_expires_at <= now

    def attempts_remaining(self) -> Bool:
        return self.attempt < self.max_attempts

    def is_terminal(self) -> Bool:
        return self.status.is_terminal()


def _is_before(a: ProcessRecord, b: ProcessRecord) -> Bool:
    """Compare the required queue key: priority DESC, then ASC fields."""
    if a.priority != b.priority:
        return a.priority > b.priority
    if a.available_at != b.available_at:
        return a.available_at < b.available_at
    if a.created_at != b.created_at:
        return a.created_at < b.created_at
    return a.id < b.id


def process_is_claimable(process: ProcessRecord, now: Float64) -> Bool:
    """Return whether ``process`` can be selected for a new claim."""
    if not process.attempts_remaining():
        return False
    if process.status == ProcessStatus.ready():
        return True
    if process.status == ProcessStatus.retry_wait():
        return process.available_at <= now
    if process.status == ProcessStatus.running():
        return process.lease_is_expired(now)
    return False


def ready_processes(
    processes: List[ProcessRecord], now: Float64
) -> List[ProcessRecord]:
    """Return claimable records in deterministic queue order.

    A selection sort is used rather than relying on a version-specific List.sort
    API, making ordering explicit and stable at the persistence boundary.
    """
    var remaining = List[ProcessRecord]()
    for process in processes:
        if process_is_claimable(process, now):
            remaining.append(process.copy())

    var ordered = List[ProcessRecord]()
    var selected = List[Bool]()
    for process in remaining:
        selected.append(False)
    while len(ordered) < len(remaining):
        var best = -1
        for index in range(len(remaining)):
            if not selected[index] and (best < 0 or _is_before(remaining[index], remaining[best])):
                best = index
        if best < 0:
            break
        selected[best] = True
        ordered.append(remaining[best].copy())
    return ordered^


def claim_process(
    process: ProcessRecord,
    worker_id: String,
    now: Float64,
    lease_seconds: Float64,
) raises -> ProcessRecord:
    """Claim a ready, due retry, or expired running process.

    The returned record is a new value; callers persist it atomically.  Claim
    increments ``attempt`` and installs the worker lease.  A live lease cannot
    be stolen by another worker.
    """
    if worker_id == "":
        raise Error("worker_id must not be empty")
    if lease_seconds <= 0.0:
        raise Error("lease_seconds must be greater than zero")
    if not process_is_claimable(process, now):
        raise Error("process is not claimable")
    if (
        process.status == ProcessStatus.running()
        and process.has_lease()
        and not process.lease_is_expired(now)
        and process.lease_owner != worker_id
    ):
        raise Error("process lease is held by another worker")

    var claimed = process.copy()
    claimed.status = ProcessStatus.running()
    claimed.attempt = process.attempt + 1
    claimed.lease_owner = worker_id
    claimed.lease_expires_at = now + lease_seconds
    return claimed^


def actor_can_transition(process: ProcessRecord, actor: String) -> Bool:
    """Check lease ownership for operations on a leased process."""
    return not process.has_lease() or process.lease_owner == actor


def transition_process(
    process: ProcessRecord,
    to_status: ProcessStatus,
    actor: String = "",
) raises -> ProcessRecord:
    """Apply a legal status transition and enforce lease ownership."""
    if not can_transition_process(process.status, to_status):
        raise Error("illegal process status transition")
    if not actor_can_transition(process, actor):
        raise Error("process lease is held by another actor")

    var transitioned = process.copy()
    transitioned.status = ProcessStatus(to_status.value)
    if (
        to_status.is_terminal()
        or to_status == ProcessStatus.waiting()
        or to_status == ProcessStatus.retry_wait()
    ):
        transitioned.lease_owner = ""
        transitioned.lease_expires_at = 0.0
    return transitioned^


def retry_is_eligible(process: ProcessRecord) -> Bool:
    """Return whether another attempt may be scheduled."""
    return (
        process.attempts_remaining()
        and (
            process.status == ProcessStatus.running()
            or process.status == ProcessStatus.failed()
        )
    )


def retry_backoff_seconds(
    process: ProcessRecord,
    base_seconds: Float64 = 1.0,
    max_seconds: Float64 = 3600.0,
) raises -> Float64:
    """Calculate capped exponential backoff for the next retry.

    Attempt one has ``base_seconds`` delay; each subsequent failed attempt
    doubles it.  No floating-point exponentiation dependency is required.
    """
    if base_seconds < 0.0:
        raise Error("base_seconds must not be negative")
    if max_seconds < base_seconds:
        raise Error("max_seconds must be at least base_seconds")
    var delay = base_seconds
    var step = 1
    while step < process.attempt:
        if delay >= max_seconds / 2.0:
            delay = max_seconds
            break
        delay = delay * 2.0
        step += 1
    if delay > max_seconds:
        return max_seconds
    return delay


def retry_process(
    process: ProcessRecord,
    now: Float64,
    actor: String = "",
    base_seconds: Float64 = 1.0,
    max_seconds: Float64 = 3600.0,
) raises -> ProcessRecord:
    """Move an eligible process to retry_wait with a computed due time."""
    if not retry_is_eligible(process):
        raise Error("process cannot be retried")
    if not actor_can_transition(process, actor):
        raise Error("process lease is held by another actor")
    var retried = process.copy()
    retried.status = ProcessStatus.retry_wait()
    retried.available_at = now + retry_backoff_seconds(process, base_seconds, max_seconds)
    retried.lease_owner = ""
    retried.lease_expires_at = 0.0
    return retried^


def expire_process(
    process: ProcessRecord,
    now: Float64,
    base_seconds: Float64 = 1.0,
    max_seconds: Float64 = 3600.0,
) raises -> ProcessRecord:
    """Resolve an expired running lease.

    Exhausted processes become failed (terminal).  Otherwise they enter
    retry_wait and can be claimed once their backoff is due.
    """
    if process.status != ProcessStatus.running() or not process.lease_is_expired(now):
        raise Error("process lease has not expired")
    if not process.attempts_remaining():
        var failed = process.copy()
        failed.status = ProcessStatus.failed()
        failed.lease_owner = ""
        failed.lease_expires_at = 0.0
        return failed^
    return retry_process(process, now, process.lease_owner, base_seconds, max_seconds)
