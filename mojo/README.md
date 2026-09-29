# Native Fala core (Mojo)

**Product version: 0.9.4** — Mojo-native engine.

Fala composes small programs into a graph. A node is one effector with
structured output. The LLM is not the spine.

- Journal is core: `JournalPort` + `InMemoryJournal`. File/SQL/JSONL sinks
  implement the same port (SQLite is the reference file sink; persistence
  is not uniform).
- Process host is core (`subprocess` adapters).
- Packages: TOML / JSON only (no YAML).
- Adapters: `subprocess`, `native_function`, `manual_homeostat`, `child_path`.

See `docs/FALA_ARCHITECTURE_STATUS.md` and root `CHANGELOG.md`.

## Setup

```bash
mise exec -- pixi run core-smoke
```

Core smokes must pass **without** SQLite.

## Layout

| Path | Role |
| --- | --- |
| `fala/status.mojo`, `processes.mojo` | pure lifecycle policy |
| `fala/correlation.mojo` | pure correlation planning |
| `fala/journal_port.mojo` | batch types |
| `fala/memory_journal.mojo` | InMemory sink |
| `fala/json.mojo`, `toml.mojo`, … | shared utilities |
| `smoke/core_*.mojo` | core-only proof |

SQLite adapter modules are intentionally **not** in this bootstrap.
