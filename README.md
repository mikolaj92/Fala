# Fala

**Product runtime: Mojo** (`mojo/fala`). One engine — organ + journal + **process host**.

```bash
# proof
mise exec -- pixi run full-smoke
mise exec -- pixi run extended-smoke
```

CPython implementation is archived under `legacy/python-engine/` (not maintained as product).
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

- SQLite is the bundled reference journal sink (`SqliteJournal` / `Correlator`).
- In-memory and JSONL sinks implement the same Journal Protocol (`TeeJournal` optional).
- The filesystem is the default reaction store.
- The CLI is the primary operator interface (`--journal` preferred; `--db` alias).
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

## Shape

The Fala architecture is built around these modules:

- `fala.runtime.AutonomousCorrelator`: embedded runtime facade (`from_journal` / `open_journal`).
- `fala.journal`: Journal Protocol, `InMemoryJournal`, `SqliteJournal`, `JsonlJournal`, `TeeJournal`, `JournalBackedBackend`, stream helpers.
- `fala.runtime_models`: Impulse-first domain models (re-exported from `runtime_backend`).
- `fala.runtime_backend.Correlator`: SQLite reference backend (still the durable default).
- `fala.runtime_backend.RuntimeBackendService`: transactional runtime service.
- `fala.reactions.FileReactionStore`: filesystem reaction store.
- `fala.models.FalaPackageSpec`: Impulse-first package schema.
- `fala.yaml_loader.load_fala_package_yaml`: Fala package loader.

## Install

Install the released wheel with `uv`:

```bash
uv add https://github.com/mikolaj92/Fala/releases/download/0.1.1/fala_runtime-0.1.1-py3-none-any.whl
uv run fala --help
```

The package installs the `fala` import package and the `fala` CLI command.
The PyPI distribution name is `fala-runtime`, because `fala` is already used by
an unrelated project on PyPI. Once the PyPI release is published, installation
can use:

```bash
uv add fala-runtime
uv run fala --help
```

## Quick Check

Create a local runtime database:

```bash
uv run fala init --journal .fala/state.sqlite --reaction-root .fala/reactions
# --db remains a supported alias for --journal
```

Create a run:

```bash
uv run fala create-run \
  --journal .fala/state.sqlite \
  --run-id run_local \
  --title "Local impulse run"
```

Record a local impulse correlation path:

```bash
uv run python examples/runtime/local_first.py .fala/state.sqlite
```

Inspect recorded events:

```bash
uv run fala events list \
  --db .fala/state.sqlite \
  --run-id run_local \
  --limit 20
```

Export a static report:

```bash
uv run fala export-html \
  --db .fala/state.sqlite \
  --run-id run_local \
  --out report.html
```

## Impulse Package Schema

Fala packages use the current schema directly:

```yaml
version: 2
id: example_correlation_path

impulse_types:
  - id: input_text
    media_types:
      - text/plain

association_kinds:
  - id: text_stats

reaction_kinds:
  - id: normalized_text
    media_types:
      - text/plain

capabilities:
  - id: normalize
    accepts_impulse_types:
      - input_text
    emits_reaction_kinds:
      - normalized_text
    emits_association_kinds:
      - text_stats

correlation_paths:
  - id: basic
    effectors:
      - id: normalize
        capability: normalize
        adapter:
          kind: python_function
          ref: examples.effectors.normalize_text

runtime:
  backend:
    kind: sqlite
    path: .fala/state.sqlite
  reaction_store:
    kind: filesystem
    root: .fala/reactions
```

Load the schema with:

```python
from fala import load_fala_package_yaml

package = load_fala_package_yaml("fala-package.yaml")
```

## CLI Surface

The local runtime is operated with `fala`:

```bash
uv run fala db init --db .fala/state.sqlite
uv run fala db migrate --db .fala/state.sqlite
uv run fala db status --db .fala/state.sqlite
uv run fala db vacuum --db .fala/state.sqlite

uv run fala create-run --db .fala/state.sqlite --run-id run_local
uv run fala runs list --db .fala/state.sqlite
uv run fala impulses list --db .fala/state.sqlite --run-id run_local
uv run fala associations list --db .fala/state.sqlite --run-id run_local
uv run fala processes list --db .fala/state.sqlite --run-id run_local
uv run fala events list --db .fala/state.sqlite --run-id run_local
uv run fala reactions record --db .fala/state.sqlite --run-id run_local --kind report --path report.txt
uv run fala reactions list --db .fala/state.sqlite --run-id run_local
uv run fala homeostats list --db .fala/state.sqlite --run-id run_local
uv run fala homeostat open --db .fala/state.sqlite --run-id run_local --kind human.review
uv run fala homeostat complete --db .fala/state.sqlite --run-id run_local --homeostat-id homeostat_123
uv run fala projections rebuild --db .fala/state.sqlite --run-id run_local
uv run fala run-until-idle --db .fala/state.sqlite --run-id run_local
uv run fala gc --db .fala/state.sqlite --reaction-root .fala/reactions --dry-run
uv run fala bridge export --db .fala/state.sqlite --run-id run_local --delivery-id bridge_123 --out bridge.json
uv run fala bridge import --db /tmp/target.sqlite --file bridge.json

uv run fala doctor --db .fala/state.sqlite --package examples/correlation-paths/basic/fala-package.yaml
uv run fala diagnose-waits --db .fala/state.sqlite --run-id run_local
uv run fala trace --db .fala/state.sqlite --run-id run_local
uv run fala replay-execution --db .fala/state.sqlite --run-id run_local --process-id process_123
uv run fala archive-run run_local --db .fala/state.sqlite --out run_local.archive.zip --retention-days 30
uv run fala archive-gc --archive-root .fala/archives --dry-run
uv run fala export-bundle --db .fala/state.sqlite --run-id run_local --out run_local.fala.zip
```

`fala schema` exposes Impulse-first contracts:

```bash
uv run fala schema fala-package
uv run fala schema impulse
uv run fala schema event
uv run fala schema homeostat
uv run fala schema projection
```

## SQLite Backend

The SQLite backend stores runtime state and event data in one local database. It
enables:

- run creation and inspection
- impulse and impulse relation storage
- ordered event append
- association and reaction metadata storage
- process scheduling and leasing
- homeostat persistence
- projection rebuilds
- command idempotency
- local bridge outbox/inbox delivery

The runtime initializes SQLite with WAL mode, foreign keys, and a busy timeout.

## Reaction Store

Reaction content belongs in a reaction store. SQLite stores metadata and refs;
the default store is a local filesystem root such as `.fala/reactions`.
`fala gc` only deletes blobs that are not referenced by any run in the SQLite
runtime, even when `--run-id` is supplied.

## Composition

Fala can reference other runtimes through `RuntimeRef`, `RunRef`, and `EventRef`.
Bridge records are delivered explicitly, without global transactions.

## Fala Docs

Start with `docs/CONCEPTUAL_MODEL.md`, then `docs/RUNTIME_SEMANTICS.md`,
`docs/SQLITE_BACKEND.md`, `docs/ADAPTER_CONTRACTS.md`, and
`docs/MULTI_FALA_COMPOSITION.md`. Version policy lives in
`docs/MIGRATIONS.md`.

Impulse-first domain-pack examples live under `examples/domain-packs`.

## Development Check

Focused Fala checks:

```bash
uv run python -m unittest \
  tests.test_fala_runtime_backend \
  tests.test_fala_package_schema \
  tests.test_runtime_backend_conformance \
  tests.test_fala_impulses
```
