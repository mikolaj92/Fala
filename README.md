# Fala

**Product runtime: Mojo** (`mojo/fala`). One engine — organ + journal + **process host**.

```bash
# proof
mise exec -- pixi run full-smoke
mise exec -- pixi run extended-smoke
```

Exclusive Mojo product — no CPython engine in tree.
No multi-runtime / fleet / `fala_runtime` peer mesh.

---

It combines two disciplines:

- **Cybernetic** — Impulses conduct through `CorrelationPath`s of Effectors;
  Associations, Reactions, and Homeostats name memory, footprint, and defensive
  waits of an autonomous correlator (Mazur/Kossecki lexicon).
- **Unix** — the core is a supervisor and **event-stream emitter**; durability
  is a Journal port with pluggable sinks. SQLite is the **reference sink**, not
  the identity of the engine. Children get separate journals; no shared `.db`
  lock for nested Fala.

The core object is an `Impulse`: a typed information impulse that moves through
process graphs. Fala records durable runs, impulses, impulse relations,
associations, reactions, events, homeostats, projections, commands, bridge records,
lineage, and audit data via the journal/backend.

The default runtime path is serverless and local:

- SQLite is the bundled reference journal sink (`NativeJournal` / SqliteJournalPort).
- In-memory and JSONL sinks implement the same Journal Protocol (`TeeJournal` optional).
- The filesystem is the default reaction store.
- The Mojo CLI is the primary operator interface.
- No Redis, Postgres, Kafka, RabbitMQ, NATS, Docker, FastAPI, Uvicorn, or web
  server is required to run a local correlation path.

**Philosophy:** [`docs/UNIX_AND_CYBERNETICS.md`](docs/UNIX_AND_CYBERNETICS.md)  
**Architecture:** [`docs/EVENT_STREAM_CORE.md`](docs/EVENT_STREAM_CORE.md) · [`docs/CYBERNETIC_MAPPING.md`](docs/CYBERNETIC_MAPPING.md)  
**Mojo port (0.2.2 event-stream layering):** [`docs/MOJO_EVENT_STREAM_MIGRATION.md`](docs/MOJO_EVENT_STREAM_MIGRATION.md)

`run_until_idle` drives processes **sequentially** (claim → execute → complete).
Fala owns journaled process state and leases; it does not orchestrate concurrent
multi-run job isolation for the host. Embedded consumers that start several
drivers in parallel must keep process ids / work roots unique (the default
correlation-path id already includes `run_id`). See
[`docs/PROCESS_RUNTIME.md`](docs/PROCESS_RUNTIME.md#execution-model-what-fala-does-not-own).

## Run (Mojo product)

```bash
git submodule update --init --recursive
make -C vendor/sqlite.fire/native
mise exec -- pixi install
mise exec -- pixi run full-smoke
mise exec -- pixi run extended-smoke
```

Process host + SQLite adapter smokes are included. See
[`docs/MOJO_EVENT_STREAM_MIGRATION.md`](docs/MOJO_EVENT_STREAM_MIGRATION.md).

## Shape (Mojo)

- `mojo/fala/journal_port.mojo` + sinks (`memory_journal`, `jsonl_journal`, SQLite via `NativeJournal`)
- `mojo/fala/memory_driver.mojo` / `native_driver.mojo` — claim → execute → complete
- `mojo/fala/native_process_host` — OS children for `subprocess` adapters
- `mojo/fala/cli.mojo` + `native_cli_surface.mojo` — operator CLI
- Package load: `native_package.mojo` (TOML/JSON)

## Development Check

```bash
mise exec -- pixi run core-smoke
mise exec -- pixi run host-smoke
mise exec -- pixi run full-smoke
```
