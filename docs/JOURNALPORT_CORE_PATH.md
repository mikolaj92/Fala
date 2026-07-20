# JournalPort core-path audit

Parent track: thin core (#94, #99).

## Hard rule

On the **core path** (accept impulse → claim process → complete / fail / wait /
homeostat terminal), durable **run and process truth** is written through
**JournalPort** batch and claim APIs (`append_batch`, `claim_next` / journal
claim helpers). Sinks materialize history; they must not invent a second
supervisor write path for process leases.

## Core-path write map

| Flow | Primary API | Durability boundary |
| --- | --- | --- |
| Accept impulse | `NativeDomainStore.accept_impulse` | Records command + events in the reference SQLite journal tables via `_domain_command_start` / `_append_domain_event_in_tx` (idempotent command keys). Memory path uses `InMemoryJournal.append_batch` via driver/runtime. |
| Claim / complete / fail / retry / wait | `native_driver` + `NativeJournal` / `JournalPort` | `claim_process` / process transitions go through journal claim and command batches — not ad-hoc process UPDATEs outside the journal helpers. |
| Homeostat open / terminal | `save_homeostat`, `transition_homeostat` (+ driver helpers) | Domain command + event rows with idempotency; status transitions are command-gated. |
| Association / reaction / relation record | `record_*` / `put_*` | Domain tables + optional journaled wrappers; reaction **bytes** live in the reaction store (refs in journal). |

## Ops / sink-only (non-core)

These may touch SQLite (or filesystem CAS) without being part of Essential Fala.
They are **not** required for `package → impulse → run_until_idle → journal`.

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

- [`EVENT_STREAM_CORE.md`](EVENT_STREAM_CORE.md) — command/event stream model  
- [`UNIX_AND_CYBERNETICS.md`](UNIX_AND_CYBERNETICS.md) — core vs journal vs sink  
- [`FALA_ARCHITECTURE_STATUS.md`](FALA_ARCHITECTURE_STATUS.md) — Essential vs ops inventory  
