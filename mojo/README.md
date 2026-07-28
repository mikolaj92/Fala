# Native Fala core (Mojo)

**Product version: 0.7.15** — Mojo-native engine.

**Journal is core:** `JournalPort` types + `InMemoryJournal` ship with the
engine. File/SQL/JSONL sinks implement the same port (SQLite is the reference
file sink).

- Process host is core (`subprocess` adapters).
- Packages: TOML / JSON only (no YAML).
- Adapters: `subprocess`, `native_function`, `manual_homeostat`.

See `docs/FALA_ARCHITECTURE_STATUS.md` and root `CHANGELOG.md`.

## Setup

```bash
# Dependencies (EmberJson, sqlite.fire) are managed dynamically. To run tests:
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
