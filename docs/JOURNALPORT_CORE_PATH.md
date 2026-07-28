# JournalPort core-path audit

Parent track: thin core (#94, #99).

## Why this boundary exists

The earlier SQLite-first runtime had two competing write paths: facade methods
mutated durable tables while event/command records described some of the same
transitions. Fala moved toward an event-first core so supervision is auditable,
replayable, and independent of a particular sink. That generic JournalPort
surface is not, however, a uniform transaction guarantee across sinks.

The current SQLite authority is the direct transactional helper set on
`NativeJournal` and `NativeDomainStore`. `NativeDomainStore` is not a
`JournalPort` implementation. `SqliteJournalPort.append_batch` dispatches only
the leading unit to `NativeJournal`; non-leading units are ignored as write
inputs, so it does not provide atomic multi-unit batch replay. Memory, JSONL,
and Tee have weaker persistence/atomicity properties: JSONL claim transitions
are not persisted to the file, and Tee mirrors appends after its primary
accepts them rather than providing a cross-sink transaction.

JournalPort has one generic authority per sink: an in-memory map, SQLite
adapter, or JSONL file/index materializes accepted records according to that
sink's implementation. Replaying the same idempotency keys is a no-op where
that sink implements the replay contract; this document does not claim uniform
multi-unit atomicity across all sinks.

## Hard rule

The core path uses the configured sink's supported APIs. In the current SQLite
core, `NativeJournal` and `NativeDomainStore` direct helpers own atomic
transactions for command/event/state operations. The driver uses journal claim
helpers for SQLite process leases, but this must not be generalized into a
claim or transaction guarantee for every JournalPort sink.

## Core-path write map

| Flow | Primary API | Durability boundary |
| --- | --- | --- |
| Accept impulse | `NativeDomainStore.accept_impulse` | Records impulse, command, and event in one reference-SQLite transaction with idempotent keys. Memory runtime uses `InMemoryJournal.append_batch`; other sinks follow their own adapter behavior. |
| Claim / complete / fail / retry / wait | `NativeJournal` helpers in SQLite; JournalPort claim/batch APIs for generic sinks | SQLite claim uses `NativeJournal.claim_next_ready` atomically and returns a `ClaimResult` representation from `SqliteJournalPort`; JSONL claim transitions remain in its in-memory index and are not persisted. |
| Homeostat open / terminal | `save_homeostat`, `transition_homeostat` (+ driver helpers) | SQLite domain command + event + state rows commit together with idempotency; this is not a uniform guarantee of every sink. |
| Association / reaction / relation record | `record_*` / `put_*` | NativeDomainStore direct domain transactions; reaction **bytes** live in the reaction store (refs in journal). |

Low-level `put_*` methods are direct SQLite writes and are test/admin or
explicit record APIs where documented; they do not establish that every
mutation routes through JournalPort. Production host paths use the applicable
native command/event helpers, while generic sinks retain their weaker
semantics.

## Ops / sink-only (non-core)

These may touch SQLite (or filesystem CAS) without being part of Essential Fala.
They are **not** required for `package → impulse → run_until_idle → configured persistence`.

| API area | Module | Notes |
| --- | --- | --- |
| `delete_run`, `run_retention`, `maintain_journal`, reaction GC | `ops_maintenance` | Admin lifecycle; may DROP append-only triggers inside a retention transaction, VACUUM, delete unreferenced blobs. |
| Bridge outbox/inbox enqueue, import, claim, deliver, retry | `ops_bridge` | Optional multi-journal composition aid; separate from organ identity. Local orchestration: `bridge_transport`. |
| `rebuild_projection(s)`, `rebuild_projections_with_command` | `ops_projections` | Projection materialization / rebuild from event watermark — sink/consumer concern. |
| Lightweight `put_projection` / get/list | `domain_store` | Read models may be written when an effector explicitly saves; heavy rebuild is ops. |

## Side-channel checklist (happy path)

| Check | Status |
| --- | --- |
| Process lease claim dual-write bypassing journal claim helpers | Must not; driver uses journal claim APIs |
| Silent process status UPDATE outside transition helpers | Must not on core path |
| Ops retention deleting runs without going through JournalPort | Allowed **as ops** — documented non-core |
| Bridge budget/status tables | Ops / composition — not core organ |
| Projection rebuild rewriting `projections` rows | Ops — rebuildable from events |

## Nested composition

Child Fala uses a **separate journal path**. Parent never writes the child’s
JournalPort. Bridge import is optional envelope merge into the parent/target
sink, not shared mutable state.

## Related docs

- [`RUNTIME_SEMANTICS.md`](RUNTIME_SEMANTICS.md) — transaction and state invariants
- [`FALA_HOST_AND_COMPOSITION.md`](FALA_HOST_AND_COMPOSITION.md) — separate-journal child composition
- [`EVENTS_AND_REPLAY.md`](EVENTS_AND_REPLAY.md) — event ordering and projection replay
- [`FALA_ARCHITECTURE_STATUS.md`](FALA_ARCHITECTURE_STATUS.md) — Essential vs ops inventory
