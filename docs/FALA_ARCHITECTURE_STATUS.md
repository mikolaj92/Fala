# Fala Architecture Status

**Product: 0.3.0** (exclusive Mojo) · **thin-core track** (#94–#100).

Fala is an embedded, **event-first**, **Mojo-native** runtime for observable
correlation paths. SQLite is the reference journal sink, not product identity.

Philosophy: [`UNIX_AND_CYBERNETICS.md`](UNIX_AND_CYBERNETICS.md) · Host boundary:
[`FALA_HOST_AND_COMPOSITION.md`](FALA_HOST_AND_COMPOSITION.md) · Mojo land:
[`MOJO_EVENT_STREAM_MIGRATION.md`](MOJO_EVENT_STREAM_MIGRATION.md) · JournalPort
audit: [`JOURNALPORT_CORE_PATH.md`](JOURNALPORT_CORE_PATH.md).

## Essential Fala (merge-gate)

Happy path: **package → impulse → run_until_idle → JournalPort**.

| Piece | Role | Primary modules |
| --- | --- | --- |
| **Organ** | Impulse ontology, correlation advance, process state machine | `correlation*`, `processes`, `status`, `models*` |
| **JournalPort** | `append_batch` / `claim_next` / load; atomic batches + sequences | `journal_port`, `memory_journal`, `sqlite_journal_port`, `jsonl_journal`, `tee_journal`, `journal` |
| **Driver + host** | claim → adapter → complete/fail/wait | `native_driver`, `native_process_host`, `adapters` |
| **Local adapters** | `subprocess`, `native_function`, `manual_homeostat` | `adapters`, `validation` |
| **Domain records (core path)** | accept impulse, record association/reaction/homeostat, put/get/list | `domain_store` (slim), `domain` |
| **Package / CLI core** | TOML package, run lifecycle, inspect one journal | `native_package`, `package`, `native_cli_surface` (core commands) |

## Optional / ops layers (not merge-gate)

Composable operators may import these; composing a small flow does **not** require them.

| Layer | Responsibility | Module |
| --- | --- | --- |
| **ops maintenance** | run retention, journal maintain, reaction CAS GC, delete_run | `ops_maintenance.mojo` (**bodies live here**) |
| **ops bridge** | outbox/inbox enqueue, import, claim/deliver/retry, budgets | `ops_bridge.mojo` (**bodies live here**; + `bridge_transport`) |
| **ops projections** | heavy projection rebuild (`run_summary`) | `ops_projections.mojo` (**bodies live here**) |
| **CLI ops surface** | `ops maintain-journal`, `ops gc`, `ops projections rebuild`, `ops bridge *` (aliases keep old names) | `native_cli_surface` progressive disclosure |

Ops free functions take `mut store: NativeDomainStore` and use the shared SQLite
connection plus private store helpers (`_require_run`, `_text`,
`_domain_command_start`, …). **Method bodies for retention/bridge/rebuild are
not on `NativeDomainStore`.** Essential Fala code paths must not require `ops_*`.

Hard rule: durable mutations on the **core path** go through **JournalPort**
(batch/claim). Ops may touch sink tables (retention VACUUM, bridge rows, rebuild
materializations) and are documented as non-core in
[`JOURNALPORT_CORE_PATH.md`](JOURNALPORT_CORE_PATH.md).

## Product tree

| Path | Role |
| --- | --- |
| `mojo/fala/` | Product engine (core + optional ops modules) |
| `mojo/smoke/` + `pixi.toml` | Proof gates (`full-smoke`, `extended-smoke`) |
| `examples/correlation-paths/basic/` | Core package example (native_function + TOML) |
| `examples/splot-integration/` | Host Splot 0.3+ via subprocess (organ outside Fala) |
| `examples/domain-packs/splot/` | Splot vocabulary package (TOML) |
| `vendor/EmberJson`, `vendor/sqlite.fire` | Mojo dependencies |

There is **no** CPython product package and **no** optional Python demos.

## Core ontology

Impulse, ImpulseType, ImpulseRelation, Association, Reaction, Event, Command,
Process, Run, Homeostat, Projection, JournalPort, Effector adapters
(`subprocess`, `native_function`, `manual_homeostat`).

## Module inventory (layer tags)

| Tag | Modules (representative) |
| --- | --- |
| **core** | `journal_port`, `memory_*`, `sqlite_journal_port`, `jsonl_journal`, `tee_journal`, `journal`, `native_driver`, `correlation*`, `processes`, `runs`, `adapters`, `domain`, `domain_store` (records only), `native_package`, `package`, `status`, `schema` (reference sink DDL) |
| **sink-ops** | `ops_maintenance` |
| **bridge** | `ops_bridge`, `bridge_transport` |
| **cli-ops** | ops section of `native_cli_surface` |
| **support** | `json`, `toml`, `sqlite`, `migration`, `validation`, `errors` |
| **domain pack** | `domain_packs/splot` (vocabulary; logic lives in external organs like Splot) |

Forbidden for Essential Fala checklist: requiring `ops_maintenance`, `ops_bridge`,
or `ops_projections` to accept an impulse or drive a claim loop.

## Status

| Area | Status |
| --- | --- |
| Journal port + InMemory / Sqlite / Jsonl / Tee | DONE |
| Process host + subprocess | DONE (`host-smoke`) |
| Package + native_function | DONE |
| CLI core + ops progressive disclosure | DONE |
| Ops extracted (maintenance / bridge / rebuild) | DONE |
| JournalPort core-path audit | DONE (`JOURNALPORT_CORE_PATH.md`) |
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
