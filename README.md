# Fala

Fala is an embedded, SQLite-first runtime for observable information flows.

The core object is a `Carrier`: a typed information carrier that moves through
process graphs. Fala records durable runs, carriers, carrier relations,
observations, artifacts, events, gates, projections, commands, bridge records,
lineage, and audit data in local state.

The default runtime path is serverless and local:

- SQLite is the bundled reference backend.
- The filesystem is the default artifact store.
- The CLI is the primary operator interface.
- No Redis, Postgres, Kafka, RabbitMQ, NATS, Docker, FastAPI, Uvicorn, or web
  server is required to run a local flow.

## Shape

The Fala architecture is built around these modules:

- `fala.carrier_runtime.FalaRuntime`: embedded runtime facade.
- `fala.runtime_backend.SQLiteRuntimeBackend`: SQLite backend and command/event
  store.
- `fala.runtime_backend.RuntimeBackendService`: transactional runtime service.
- `fala.artifacts.FileArtifactStore`: filesystem artifact store.
- `fala.models.CarrierWorkflowPackageSpec`: Carrier-first package schema.
- `fala.yaml_loader.load_carrier_workflow_package_yaml`: Carrier package loader.

## Install

The PyPI distribution is `fala-runtime`. The import package and CLI command are
both `fala`.

```bash
uv add fala-runtime
uv run fala --help
```

## Quick Check

Create a local runtime database:

```bash
uv run fala init --db .fala/state.sqlite --artifact-root .fala/artifacts
```

Create a run:

```bash
uv run fala create-run \
  --db .fala/state.sqlite \
  --run-id run_local \
  --title "Local carrier run"
```

Record a local Carrier flow:

```bash
uv run python examples/carrier-runtime/local_first.py .fala/state.sqlite
```

Bridge a Carrier between two local Fala runtimes:

```bash
uv run python examples/multi-fala/basic/local_bridge.py .fala/multi-fala-basic
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

## Carrier Package Schema

Carrier packages use the current schema directly:

```yaml
version: 2
id: example_flow

carrier_types:
  - id: input_text
    media_types:
      - text/plain

observation_kinds:
  - id: text_stats

artifact_kinds:
  - id: normalized_text
    media_types:
      - text/plain

capabilities:
  - id: normalize
    accepts_carrier_types:
      - input_text
    emits_artifact_kinds:
      - normalized_text
    emits_observation_kinds:
      - text_stats

flows:
  - id: basic
    steps:
      - id: normalize
        capability: normalize
        adapter:
          kind: python_function
          ref: examples.steps.normalize_text

runtime:
  backend:
    kind: sqlite
    path: .fala/state.sqlite
  artifact_store:
    kind: filesystem
    root: .fala/artifacts
```

Load the schema with:

```python
from fala import load_carrier_workflow_package_yaml

package = load_carrier_workflow_package_yaml("carrier-package.yaml")
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
uv run fala runtimes create-pool --db .fala/state.sqlite --pool-id local_pool --policy round_robin --runtime-json '{"id":"target","uri":"sqlite:///tmp/target.sqlite"}'
uv run fala runtimes add-policy --db .fala/state.sqlite --pool-id local_pool --carrier-type source_payload --budget-json '{"runtime_hops":1,"carrier_count":1}'
uv run fala carriers list --db .fala/state.sqlite --run-id run_local
uv run fala observations list --db .fala/state.sqlite --run-id run_local
uv run fala processes list --db .fala/state.sqlite --run-id run_local
uv run fala events list --db .fala/state.sqlite --run-id run_local
uv run fala artifacts record --db .fala/state.sqlite --run-id run_local --kind report --path report.txt
uv run fala artifacts list --db .fala/state.sqlite --run-id run_local
uv run fala gates list --db .fala/state.sqlite --run-id run_local
uv run fala gate open --db .fala/state.sqlite --run-id run_local --kind human.review
uv run fala gate complete --db .fala/state.sqlite --run-id run_local --gate-id gate_123
uv run fala projections rebuild --db .fala/state.sqlite --run-id run_local
uv run fala run-until-idle --db .fala/state.sqlite --run-id run_local
uv run fala gc --db .fala/state.sqlite --artifact-root .fala/artifacts --dry-run
uv run fala bridge export --db .fala/state.sqlite --run-id run_local --delivery-id bridge_123 --out bridge.json
uv run fala bridge import --db /tmp/target.sqlite --file bridge.json

uv run fala doctor --db .fala/state.sqlite --package examples/pipelines/basic/carrier-package.yaml
uv run fala diagnose-waits --db .fala/state.sqlite --run-id run_local
uv run fala trace --db .fala/state.sqlite --run-id run_local
uv run fala replay-execution --db .fala/state.sqlite --run-id run_local --process-id process_123
uv run fala archive-run run_local --db .fala/state.sqlite --out run_local.archive.zip --retention-days 30
uv run fala archive-gc --archive-root .fala/archives --dry-run
uv run fala export-bundle --db .fala/state.sqlite --run-id run_local --out run_local.fala.zip
```

`fala schema` exposes Carrier-first contracts:

```bash
uv run fala schema carrier-package
uv run fala schema carrier
uv run fala schema event
uv run fala schema gate
uv run fala schema projection
```

## SQLite Backend

The SQLite backend stores runtime state and event data in one local database. It
enables:

- run creation and inspection
- carrier and carrier relation storage
- ordered event append
- observation and artifact metadata storage
- process scheduling and leasing
- gate persistence
- projection rebuilds
- command idempotency
- local bridge outbox/inbox delivery

The runtime initializes SQLite with WAL mode, foreign keys, and a busy timeout.

## Artifact Store

Artifact content belongs in an artifact store. SQLite stores metadata and refs;
the default store is a local filesystem root such as `.fala/artifacts`.
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

Carrier-first domain-pack examples live under `examples/domain-packs`.

## Development Check

Focused Fala checks:

```bash
uv run python -m unittest \
  tests.test_fala_runtime_backend \
  tests.test_carrier_package_schema \
  tests.test_runtime_backend_conformance \
  tests.test_fala_carriers
```
