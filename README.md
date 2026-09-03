# Fala

**Version 0.7.32** — Mojo-native engine with an optional thin Python host
binding; local autonomous Correlator, cybernetic mediation, event-first memory,
and a POSIX process host.

## Local autonomous Correlator

Fala is a cybernetic mediator for autonomous organs. It accepts typed
**Impulses**, conducts them through named contracts between **Effectors**, and
records associations and reactions as an observable memory trace. The runtime
is event-first: JournalPort batches carry commands and events, while sinks
provide durable storage.

Unix process composition is the implementation boundary, not the product
identity. Fala hosts local children, keeps each organ's journal separate, and
drives one run to idle through its embedded/library API; `run_until_idle` is
not a standalone CLI command.

```text
you compose:   TOML/JSON package → Impulse → embedded run_until_idle → JournalPort
Fala mediates: cybernetic contracts + conduction + process host
organs live:   native_function | subprocess | manual_homeostat
```

## Core picture

```text
Impulse / package (TOML or JSON)
        │
        ▼
  CorrelationPath (named contracts)
        │
        ├── native_function   (in-process Mojo registry)
        ├── subprocess        (OS child + JSON manifest boundary)
        └── manual_homeostat  (operator wait and regulation)
        │
        ▼
  JournalPort → InMemory | SQLite | JSONL | Tee
        │
        ▼
  events · associations · reactions · processes · projections
```

SQLite is the bundled reference sink; memory, JSONL, and tee sinks implement
the same JournalPort. Reaction bytes live in the local reaction store, while
the journal records metadata and references.

## What it is for

| Use | Example |
| --- | --- |
| Local correlation paths | ingest → enrich → export as durable processes |
| Host Mojo tools | run Splot or any argv child as a subprocess effector |
| Domain packs | Signals, Splot, and Takt vocabulary on Fala records |
| Observable runs | journaled leases, retries, homeostats, and projections |
| Embedded / CLI | embedded code drives a run to idle; CLI creates/lists/inspects one local run |

## What it is not

- Not a fleet or multi-runtime peer mesh
- Not Redis/Postgres/Kafka; local journals and filesystem reactions are the
  default

## Mojo-native engine

| Surface | Current contract |
| --- | --- |
| Engine | Mojo only (`mojo/fala/`) |
| Host binding | Optional thin Python package (`python/fala/`), a JSON bridge to Mojo—not a second engine |
| Packages | TOML or canonical JSON |
| Adapters | `subprocess`, `native_function`, `manual_homeostat` |
| Journal | `JournalPort`, memory/SQLite/JSONL/tee sinks |
| Proof | Mojo smokes under `mojo/smoke/` and `pixi.toml` |

```text
mojo/fala/      Mojo Correlator, JournalPort, driver, host, packages, CLI
python/fala/    optional host binding and subprocess-effector SDK
mojo/smoke/     executable gates
examples/       TOML packages and domain vocabulary
docs/           architecture and contracts
vendor/         dynamically managed EmberJson and sqlite.fire dependencies
tools/          native smoke helpers
```

## Adapters and process execution

| Kind | Role |
| --- | --- |
| `subprocess` | argv child; manifest in `input/`, result in `output/result.json` |
| `native_function` | in-process Mojo callable from the native registry |
| `manual_homeostat` | durable operator wait and explicit completion |

`run_until_idle` is the embedded/library API that drives processes sequentially
(claim → execute → complete), not a standalone CLI command. Each process has a
run-scoped identity, lease, and isolated work directory. The native CLI exposes
the durable create/lifecycle/list/inspect operations separately and requires
`--db`, `--run-id`, and `--now` where those commands need them. Independent work
can use multi-claim batches or separate Fala instances with separate journals.
See [`docs/PROCESS_RUNTIME.md`](docs/PROCESS_RUNTIME.md).

Fala is a mediator, not a workflow tyrant: terminal upstreams conduct success
or error payloads to dependents, and the receiving effector decides what the
payload means. A failed upstream does not silently cancel its dependents.
Package authors may add `when = { upstream, path, equals }` to select a branch
from a successful direct-upstream JSON scalar. Fala records a nonmatching branch
as `skipped`; it does not assign meaning to the compared domain value.

## Quick proof

Requires Pixi/Mojo (see `pixi.toml`). The mill gate is the same command, declared as `[tool.lokay] test` in `pyproject.toml`:

```bash
mise exec -- pixi run full-smoke
mise exec -- pixi run core-smoke    # no SQLite
mise exec -- pixi run host-smoke    # process host and subprocess boundary
```

## Examples

| Path | What |
| --- | --- |
| `examples/correlation-paths/basic/` | TOML package with native effectors |
| `examples/domain-packs/splot/` | Splot vocabulary on Fala records |
| `examples/splot-integration/` | Splot 0.3.1+ hosted as a subprocess |

```bash
mise exec -- pixi run example-basic-native
mise exec -- pixi run splot-domain
mise exec -- pixi run splot-integration
```

Fala does not import Splot. Splot is an optional child product; Fala owns
conduction, process hosting, and the journal.

### Durable in-process callbacks (Python)

For a resident Python component that must record one attempt without a subprocess,
create the run through the normal durable lifecycle and call `record_in_process`:

```python
result = fala.record_in_process(
    db_path="journal.sqlite",
    run_id="existing-run",
    process_id="attempt-42",
    inputs={"checkpoint": 41},
    metadata={"component": "importer"},
    operation=lambda: import_one_batch(),
)
```

The callback is invoked once. Its JSON-recordable result is stored in one succeeded
process row and returned unchanged; exceptions produce a failed row and are re-raised.
Invalid JSON diagnostics/results fail closed. Executions sharing a journal are
non-blocking single-flight. This primitive records only the callback attempt: callers
continue to own run creation and finalization policy.

## Docs

### Identity and philosophy

| Doc | Focus |
| --- | --- |
| [`AGENTS.md`](AGENTS.md) | composition mandate for agents and consumers |
| [`CONCEPTUAL_MODEL.md`](docs/CONCEPTUAL_MODEL.md) | canonical ontology and conduction rules |
| [`CYBERNETIC_MAPPING.md`](docs/CYBERNETIC_MAPPING.md) | historical → current lexicon and Mojo surfaces |
| [`UNIX_AND_CYBERNETICS.md`](docs/UNIX_AND_CYBERNETICS.md) | Unix composition and cybernetic synthesis |
| [`DOMAIN_PACKS.md`](docs/DOMAIN_PACKS.md) | domain vocabulary boundaries |
| [`TAKT_DOMAIN_PACK.md`](docs/TAKT_DOMAIN_PACK.md) | Takt domain adapter |

### Runtime and boundaries

| Doc | Focus |
| --- | --- |
| [`RUNTIME_SEMANTICS.md`](docs/RUNTIME_SEMANTICS.md) | transaction and state invariants |
| [`PROCESS_RUNTIME.md`](docs/PROCESS_RUNTIME.md) | claims, leases, retries, and execution |
| [`ADAPTER_CONTRACTS.md`](docs/ADAPTER_CONTRACTS.md) | subprocess and effector wire boundary |
| [`FALA_HOST_AND_COMPOSITION.md`](docs/FALA_HOST_AND_COMPOSITION.md) | process-host composition |
| [`JOURNALPORT_CORE_PATH.md`](docs/JOURNALPORT_CORE_PATH.md) | JournalPort core path |
| [`SQLITE_BACKEND.md`](docs/SQLITE_BACKEND.md) | reference sink details |
| [`REACTIONS_AND_REFERENCES.md`](docs/REACTIONS_AND_REFERENCES.md) | reaction bytes and references |

### Operations and assurance

| Doc | Focus |
| --- | --- |
| [`EVENTS_AND_REPLAY.md`](docs/EVENTS_AND_REPLAY.md) | event ordering and replay |
| [`MIGRATIONS.md`](docs/MIGRATIONS.md) | schema and package migration |
| [`SECURITY.md`](docs/SECURITY.md) | trust boundary and subprocess safety |
| [`FALA_ARCHITECTURE_STATUS.md`](docs/FALA_ARCHITECTURE_STATUS.md) | current architecture status |
| [`SPLOT_DOMAIN_PACK.md`](docs/SPLOT_DOMAIN_PACK.md) | Splot vocabulary and host integration |

[`CHANGELOG.md`](CHANGELOG.md) contains release history.

## License

MIT — see [`LICENSE`](LICENSE).
