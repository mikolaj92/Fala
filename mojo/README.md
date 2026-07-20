# Native Fala core (Mojo)

**Strategy:** port **core first** (event-stream + cybernetic pure organ +
InMemory journal). SQLite and other sinks are **adapters** added later.

This tree is a fresh 0.2.2-aligned bootstrap:

- lifted pure modules from the historical `mojo` branch where useful
- new `journal_port` + `memory_journal` (no `sqlite.fire`)
- Python package under `src/fala` remains the 0.2.2 reference/oracle

See `docs/MOJO_EVENT_STREAM_MIGRATION.md`.

## Setup

```bash
git submodule update --init --recursive
mise exec -- pixi install --locked   # or: pixi install --locked
mise exec -- pixi run core-smoke
```

Core smokes must pass **without** SQLite.

## Layout

| Path | Role |
| --- | --- |
| `fala/status.mojo`, `processes.mojo` | pure lifecycle policy |
| `fala/correlation.mojo` | pure correlation planning |
| `fala/journal_port.mojo` | batch types (Journal Protocol) |
| `fala/memory_journal.mojo` | InMemory sink |
| `fala/json.mojo`, `toml.mojo`, … | shared utilities |
| `smoke/core_*.mojo` | core-only proof |

SQLite adapter modules are intentionally **not** in this bootstrap.
