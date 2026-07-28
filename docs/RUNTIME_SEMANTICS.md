# Runtime Semantics

The runtime's generic JournalPort types describe idempotent command/event
intent and ordered batches, but transaction guarantees depend on the sink. In
the current SQLite core, `NativeJournal` and `NativeDomainStore` direct helpers
own atomic command/event/state transactions. `NativeDomainStore` does not
implement JournalPort. `SqliteJournalPort.append_batch` consumes only the
leading unit and delegates to those helpers; it does not make a multi-unit
batch atomic. Memory, JSONL, and Tee have weaker persistence/atomicity
semantics, including JSONL claim transitions that are not persisted.

A runtime **Command** is the idempotent write intent. An **Event** records the
ordered, command-linked fact produced by accepting that intent. A generic batch
may carry multiple commands and events, but this document does not promise
that every sink applies every unit atomically. Replaying an idempotency key is
defined by the selected sink; SQLite command helpers preserve their replay
behavior.

Run cancellation is a first-class `run.cancel` command and emits
`run.cancel_requested`.

In the current SQLite core, run creation commits the run, `run.create` command,
and `run.created` event in one transaction. Direct
`submit_command(run.create)` is rejected. Run status transitions likewise
commit their command and event together. Other command submission and all
run-scoped writes require the target run to already exist.

Impulse acceptance commits the impulse, `impulse.accept` command, and
`impulse.accepted` event together through `NativeDomainStore`. Impulse type
registration, impulse relation recording, association recording, reaction
recording, process scheduling and status transitions follow the same SQLite
command/event/state transaction pattern where their native helpers are used.
Homeostat creation and terminal transitions use the same SQLite pattern.

Projection save and rebuild commands commit their read-model writes in the
same local SQLite transaction. Bridge outbox enqueue/deliver and inbox import
commit their local delivery record, command, and event together; cross-organ
delivery does not use a global transaction. These are SQLite guarantees, not
promises shared by every JournalPort sink.

## Process execution
- In the SQLite core, `NativeJournal.claim_next_ready` atomically reaps
  expired leases, selects one claimable process, updates its lease, and appends
  the claim command/event; `SqliteJournalPort.claim_next` returns a
  `ClaimResult` representation of that native claim. Other sinks may differ;
  JSONL claim transitions are not persisted to the JSONL file.
- Claimed processes become `running` under a worker lease.
- Adapters return completed output or an explicit waiting state.
- Waiting processes persist as `waiting`.
- Failed attempts retry while attempts remain, otherwise they become `failed`.
- Cancellation and timeout move non-terminal processes to `cancelled` or
  `timed_out` and clear worker leases.

Run, process, and homeostat transitions are validated by the Correlator before
their SQLite command/event batch is accepted. Illegal terminal rewrites are
rejected, except when the same idempotent command is replayed.

Homeostats move from `open` to exactly one terminal status: `completed`,
`cancelled`, or `expired`.
