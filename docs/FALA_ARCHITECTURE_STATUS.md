# Fala Architecture Status

**Product: 0.7.28** · Mojo-native engine + optional thin Python host binding.

Fala is a local autonomous Correlator: a cybernetic organ that conducts
Impulses between Effectors and records their Associations and Reactions. Its
implementation is an embedded, event-first Mojo runtime. SQLite is the
reference JournalPort sink, not product identity.

Philosophy: [`UNIX_AND_CYBERNETICS.md`](UNIX_AND_CYBERNETICS.md) · Host boundary:
[`FALA_HOST_AND_COMPOSITION.md`](FALA_HOST_AND_COMPOSITION.md) · Cybernetic
lexicon: [`CYBERNETIC_MAPPING.md`](CYBERNETIC_MAPPING.md) · JournalPort audit:
[`JOURNALPORT_CORE_PATH.md`](JOURNALPORT_CORE_PATH.md).

## Essential Fala

Happy path: **package → impulse → run_until_idle → configured persistence**.
The generic `JournalPort` types describe batch/claim/load operations, but they
do not make every persistence sink equivalent. In the current SQLite core,
`NativeJournal` and `NativeDomainStore` own the direct transactional helpers;
`NativeDomainStore` is not a `JournalPort` implementation.

| Piece | Role | Primary modules |
| --- | --- | --- |
| **Organ** | Impulse ontology, correlation advance, process state machine | `correlation*`, `processes`, `status`, `models*` |
| **JournalPort contract** | Generic `append_batch` / `claim_next` / load surface | `journal_port`, `memory_journal`, `sqlite_journal_port`, `jsonl_journal`, `tee_journal` |
| **SQLite core persistence** | Native command/event/state and lease transactions | `journal` (`NativeJournal`), `domain_store` (`NativeDomainStore`) |
| **Driver + host** | claim → adapter → complete/fail/wait | `native_driver`, `native_process_host`, `adapters` |
| **Local adapters** | `subprocess`, `native_function`, `manual_homeostat` | `adapters`, `validation` |
| **Domain records (core path)** | accept impulse, record association/reaction/homeostat, put/get/list | `domain_store` (direct SQLite helpers), `domain` |
| **Package / CLI core** | TOML package, run lifecycle, inspect one journal | `native_package`, `package`, `native_cli_surface` (core commands) |
The native CLI includes implemented `init`. `schema fala-package` is listed
only as a reserved native-boundary schema encoder, not an implemented command.
`run_until_idle` is an embedded/library API, not a standalone CLI command.

## Optional / ops layers

Composable operators may import these; composing a small flow does **not** require them.

| Layer | Responsibility | Module |
| --- | --- | --- |
| **ops maintenance** | run retention, journal maintain, reaction CAS GC, delete_run | `ops_maintenance.mojo` (**bodies live here**) |
| **ops bridge** | outbox/inbox enqueue, import, claim/deliver/retry, budgets | `ops_bridge.mojo` (**bodies live here**; + `bridge_transport`) |
| **ops projections** | heavy projection rebuild (`run_summary`) | `ops_projections.mojo` (**bodies live here**) |
| **CLI ops surface** | `ops maintain-journal`, `ops gc`, `ops projections rebuild`, `ops bridge list/deliver/export/import` | `native_cli_surface` progressive disclosure |

Ops free functions take `mut store: NativeDomainStore` and use the shared SQLite
connection plus private store helpers (`_require_run`, `_text`,
`_domain_command_start`, …). **Method bodies for retention/bridge/rebuild are
not on `NativeDomainStore`.** Essential Fala code paths must not require `ops_*`.

Current SQLite guarantees are deliberately narrower than the generic port
surface: `NativeJournal` and `NativeDomainStore` direct helpers own the SQLite
transactions for their command/event/state operations. `SqliteJournalPort`
`append_batch` consumes only the leading unit and delegates to those helpers;
it does not provide atomic multi-unit batch replay. The generic JournalPort
contract does not imply that memory, JSONL, or Tee provide SQLite-equivalent
durability or multi-unit atomicity. `NativeDomainStore` does not implement
JournalPort.

Core mutations use the current sink's supported APIs; this does not mean every
mutation routes through JournalPort or that every sink provides the same
transaction guarantees. SQLite core command/event/state operations retain
their direct transaction guarantees. Ops may touch sink tables (retention
VACUUM, bridge rows, rebuild materializations) and are documented as non-core
in [`JOURNALPORT_CORE_PATH.md`](JOURNALPORT_CORE_PATH.md).

## Product-line history

The engine became Mojo-native in 0.3 to keep one authoritative implementation
of command/event transactions, claims, and process supervision. The former
CPython engine, runtime service, Python adapter, and fleet surface were removed
rather than maintained as a second semantics. Python returned incrementally as
a deliberately thin host boundary: memory hosting in 0.5, subprocess-effector
SDK and SQLite opening in 0.6, and durable package hosting in 0.7. The binding
serializes requests into Mojo; it does not duplicate the engine.

This cutover followed the event-first split: graph/process supervision no
longer owns SQLite directly. The generic JournalPort surface remains useful for
memory, JSONL, Tee, and the SQLite adapter, but their persistence and atomicity
semantics are not uniform. The current SQLite authority is the direct
transactional helper set on `NativeJournal` and `NativeDomainStore`; the
historical reasons and discarded plans are indexed in [`MIGRATIONS.md`](MIGRATIONS.md);
release chronology remains in [`CHANGELOG.md`](../CHANGELOG.md).

## Product tree

| Path | Role |
| --- | --- |
| `mojo/fala/` | Product engine (core + optional ops modules) |
| `python/fala/` | Optional thin host binding and subprocess-effector SDK; JSON boundary into Mojo |
| `mojo/smoke/` + `pixi.toml` | Proof gates (`full-smoke`, `extended-smoke`) |
| `examples/correlation-paths/basic/` | Core package example (native_function + TOML) |
| `examples/splot-integration/` | Host Splot 0.3+ via subprocess (organ outside Fala) |
| `examples/domain-packs/splot/` | Splot vocabulary package (TOML) |
| `vendor/` | Gitignored de-vendored Mojo dependencies (`EmberJson`, `sqlite.fire`) |

The distribution ships an **optional thin Python host binding**. There is no
CPython engine/product runtime and no Python engine demo tree.

Fala has no web application or frontend asset surface. Authentication, session,
account, admin, and platform chrome are outside this product boundary; the
platform COMPAT UI contract therefore does not apply to the Mojo engine or its
thin Python JSON host binding. See [Fala host and composition](FALA_HOST_AND_COMPOSITION.md#web-and-platform-ui-boundary).


## Core ontology

Impulse, ImpulseType, ImpulseRelation, Association, Reaction, Event, Command,
Process, Run, Homeostat, Projection, JournalPort, Effector adapters
(`subprocess`, `native_function`, `manual_homeostat`).

## Module inventory (layer tags)

| Tag | Modules (representative) |
| --- | --- |
| **core** | `journal_port`, `memory_*`, `sqlite_journal_port`, `jsonl_journal`, `tee_journal`, `journal` (`NativeJournal`), `native_driver`, `correlation*`, `processes`, `runs`, `adapters`, `domain`, `domain_store` (`NativeDomainStore`), `native_package`, `package`, `status`, `schema` (reference sink DDL) |
| **sink-ops** | `ops_maintenance` |
| **bridge** | `ops_bridge`, `bridge_transport` |
| **cli-ops** | ops section of `native_cli_surface` |
| **support** | `json`, `toml`, `sqlite`, `migration`, `validation`, `errors` |
| **domain pack** | `domain_packs/splot` (vocabulary; logic lives in external organs like Splot) |

Essential Fala must not require `ops_maintenance`, `ops_bridge`, or
`ops_projections` to accept an impulse or drive a claim loop.

## Status

| Area | Status |
| --- | --- |
| JournalPort and sinks | IMPLEMENTED (generic surface; sink persistence and atomicity differ) |
| SQLite NativeJournal / NativeDomainStore transactions | IMPLEMENTED (current SQLite authority) |
| JournalPort core-path documentation | DONE (current sink boundaries recorded in `JOURNALPORT_CORE_PATH.md`) |
| Process host + subprocess | DONE (`host-smoke`) |
| Package + native_function | DONE |
| CLI core + ops progressive disclosure | DONE |
| Ops extracted (maintenance / bridge / rebuild) | DONE |
| Local bridge deliver + file handoff | DONE (ops) |
| Fleet / RuntimePool / `fala_runtime` | REMOVED |
| CPython engine | REMOVED |
| `python_function` adapter | REMOVED (unknown kind) |
| Domain pack Splot | DONE (`domain_packs/splot` + `splot-integration`) |
| Domain pack Signals | DONE (`domain_packs/signals` + `signals-domain` smoke) |
| Domain pack Takt | DONE (`domain_packs/takt` + `takt-domain` smoke; engine in sibling takt 0.2+) |
| Process host POSIX (Darwin + Linux) | DONE (Linux `.so` + `/proc/self/exe`; Darwin smoke) |
| Multi-claim / multi-workspace composition | DONE (`drive_ready_batch`, `claims_per_round`, multi MemoryDriver) |
| Homeostat rearm (#68) + EV regulation | DONE (`rearm_homeostat`, Signals `regulation_decision`) |
| Composer mental model docs (#34) | DONE (README lead + multi-organ example) |

## Proof

```bash
mise exec -- pixi run full-smoke
mise exec -- pixi run extended-smoke
```
