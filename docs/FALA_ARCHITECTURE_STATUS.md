# Fala Architecture Status

Fala is an embedded, **event-first**, **Mojo-native** runtime for observable
correlation paths. SQLite is the reference journal sink, not product identity.

Philosophy: [`UNIX_AND_CYBERNETICS.md`](UNIX_AND_CYBERNETICS.md) · Host boundary:
[`FALA_HOST_AND_COMPOSITION.md`](FALA_HOST_AND_COMPOSITION.md) · Mojo land:
[`MOJO_EVENT_STREAM_MIGRATION.md`](MOJO_EVENT_STREAM_MIGRATION.md).

## Product tree

| Path | Role |
| --- | --- |
| `mojo/fala/` | **Only product engine** |
| `mojo/smoke/` + `pixi.toml` | Proof gates (`full-smoke`, `extended-smoke`) |
| `examples/correlation-paths/basic/` | Core package example (native_function + TOML) |
| `examples/optional/` | Domain packs / demos — not core |
| `vendor/EmberJson`, `vendor/sqlite.fire` | Mojo dependencies |

There is **no** `src/fala` CPython product package.

## Core ontology

Impulse, ImpulseType, ImpulseRelation, Association, Reaction, Event, Command,
Process, Run, Homeostat, Projection, JournalPort, Effector adapters
(`subprocess`, `native_function`, `manual_homeostat`).

## Status

| Area | Status |
| --- | --- |
| Journal port + InMemory / Sqlite / Jsonl (rehydrate) / Tee | DONE |
| Process host + subprocess | DONE (`host-smoke`) |
| Package + native_function | DONE |
| CLI (native) | DONE |
| Local bridge deliver + file handoff | DONE |
| Fleet / RuntimePool / `fala_runtime` | REMOVED |
| CPython engine | REMOVED |
| Domain pack Splot | DONE (`domain_packs/splot`, outside organ identity) |
| Domain pack Signals | open |

## Proof

```bash
mise exec -- pixi run full-smoke
mise exec -- pixi run extended-smoke
```
