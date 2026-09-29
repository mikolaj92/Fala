# Runtime Semantics

JournalPort types describe idempotent command/event intent and ordered
batches. Transaction guarantees depend on the sink. In the SQLite core,
`NativeJournal` and `NativeDomainStore` direct helpers own atomic
command/event/state transactions. `NativeDomainStore` does not implement
JournalPort. `SqliteJournalPort.append_batch` consumes only the leading unit.
Memory, JSONL, and Tee are weaker; JSONL claim transitions are not persisted.

A **Command** is the idempotent write intent. An **Event** is the ordered,
command-linked fact produced by accepting that intent. Replaying an
idempotency key is defined by the selected sink.

Run cancellation is `run.cancel` and emits `run.cancel_requested`.

In SQLite, run creation commits the run, `run.create` command, and
`run.created` event in one transaction. Direct `submit_command(run.create)`
is rejected. Impulse acceptance, process scheduling, homeostat
create/terminal, association/reaction recording, and projection save follow
the same command/event/state pattern where their native helpers are used.
Bridge enqueue/deliver is local only; there is no global cross-organ
transaction.

Current command/event pairs include `impulse.accept`/`impulse.accepted`,
`association.record`/`association.recorded`,
`reaction.record`/`reaction.recorded`, and
`violation.record`/`violation.recorded`. Homeostat commands are
`homeostat.save`, `homeostat.open`, `homeostat.complete`,
`homeostat.cancel`, and `homeostat.expire`. Terminal process events include
`process.completed`, `process.cancelled`, and `process.timed_out`.

## Process execution

- SQLite `NativeJournal.claim_next_ready` atomically reaps expired leases,
  selects one claimable process, updates its lease, and appends the claim
  command/event. Other sinks may differ.
- Claimed processes become `running` under a worker lease.
- Adapters return completed output or an explicit waiting state.
- Waiting processes persist as `waiting`.
- Failed attempts retry while attempts remain, otherwise they become `failed`.
- Cancellation and timeout move non-terminal processes to `cancelled` or
  `timed_out` and clear worker leases.

Illegal terminal rewrites are rejected, except when the same idempotent
command is replayed. Homeostats move from `open` to exactly one of
`completed`, `cancelled`, or `expired`.

The SQLite event stream is append-only (triggers reject direct event updates
and deletes). Inspection: `fala events validate-schema`, `fala trace`,
`fala projections rebuild` (ops). Replay here means history and projections,
not re-execution, report export, or archiving. Memory/JSONL/Tee do not share
SQLite replay guarantees.
