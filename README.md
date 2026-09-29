# Fala

**Version 0.9.4** — Mojo engine, optional thin Python host.

Fala composes small programs into a graph. A node is one effector: one job,
one structured answer. An LLM can sit on a node; it is not the spine.

You author a TOML or JSON package. Fala asks each node for its answer under a
named `output_schema`, records what came back, and may ask again or stop the
OS process. It does not enter the child. A child crash stays in the child.

```text
package (TOML/JSON)
        │
        ▼
  CorrelationPath  =  the graph
        │
        ├── native_function   in-process Mojo callable
        ├── subprocess        argv child, JSON on the filesystem
        ├── manual_homeostat  operator wait
        └── child_path        nested package, compiled to argv
        │
        ▼
  JournalPort  →  memory | SQLite | JSONL | tee
```

SQLite is the bundled reference sink. Memory, JSONL, and tee implement the
same port with weaker persistence. Reaction bytes live in a local store; the
journal keeps metadata and references.

## What it is for

| Use | Example |
| --- | --- |
| Local correlation paths | ingest → enrich → export as durable processes |
| Host a sibling tool | run Splot or any argv child as a subprocess effector |
| Domain vocabulary | Signals, Splot, and Takt names on Fala records |
| Observable runs | journaled leases, retries, homeostats, and projections |

## What it is not

- Not an LLM agent framework. Models are optional effectors.
- Not a fleet or multi-runtime peer mesh.
- Not Redis/Postgres/Kafka. Local journals are the default.

## Engine

| Surface | Contract |
| --- | --- |
| Engine | Mojo only (`mojo/fala/`) |
| Host binding | Optional `python/fala/` JSON bridge — not a second engine. An installed wheel keeps the engine at `fala/mojo/` and patches at `fala/patches/`. Checkout builds still use `FALA_HOME`. |
| Packages | TOML or canonical JSON |
| Adapters | `subprocess`, `native_function`, `manual_homeostat`, `child_path` |
| Journal | `JournalPort`; memory / SQLite / JSONL / tee sinks |
| Proof | Mojo smokes under `mojo/smoke/` and `pixi.toml` |

`run_until_idle` is the embedded parent loop (claim → ask → record), not a
CLI verb. The native CLI creates, lists, and inspects one local run and
requires `--db`, `--run-id`, and `--now` where those commands need them.

Every effector declares a non-empty `output_schema` that names the answer.
`{ type = "object" }` is not a contract. Terminal upstreams conduct success
or error payloads; the receiving effector decides what they mean. Package
authors may add `when = { upstream, path, equals }` to select a branch from a
successful direct-upstream JSON scalar. A nonmatching branch is `skipped`.

Repeated finite topology can be authored once with `path_templates` and a
bounded `expansion`. Loading materializes ordinary effectors before run
creation. `max_items` is mandatory. See
[`docs/PROCESS_RUNTIME.md`](docs/PROCESS_RUNTIME.md).

## Quick proof

macOS ARM64 (`osx-arm64`) and Linux ARM64 (`linux-aarch64`, glibc 2.29+).
Requires Pixi/Mojo (see `pixi.toml`):

```bash
mise exec -- pixi run full-smoke
mise exec -- pixi run core-smoke      # no SQLite
mise exec -- pixi run host-smoke      # process host and subprocess boundary
mise exec -- pixi run python-host-api # public Python host binding
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

Fala does not import Splot. Splot is an optional child product.

### Durable in-process callbacks (Python)

For a resident Python component that must record one attempt without a
subprocess, create the run through the normal durable lifecycle and call
`record_in_process`:

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

The callback runs once. Its JSON-recordable result is stored in one succeeded
process row and returned unchanged; exceptions produce a failed row and are
re-raised. Invalid JSON fails closed. Executions sharing a journal are
non-blocking single-flight. Callers still own run creation and finalization.

## Docs

| Doc | Focus |
| --- | --- |
| [`PROCESS_RUNTIME.md`](docs/PROCESS_RUNTIME.md) | claims, leases, retries, `when`, expansion |
| [`ADAPTER_CONTRACTS.md`](docs/ADAPTER_CONTRACTS.md) | subprocess and `child_path` wire |
| [`EFFECTOR_PROTOCOL.md`](docs/EFFECTOR_PROTOCOL.md) | parent–child envelope |
| [`OUTPUT_CONTRACTS.md`](docs/OUTPUT_CONTRACTS.md) | `output_schema` subset |
| [`FALA_HOST_AND_COMPOSITION.md`](docs/FALA_HOST_AND_COMPOSITION.md) | process host, graph CLI, Python binding |
| [`RUNTIME_SEMANTICS.md`](docs/RUNTIME_SEMANTICS.md) | command/event transactions |
| [`JOURNALPORT_CORE_PATH.md`](docs/JOURNALPORT_CORE_PATH.md) | JournalPort vs SQLite helpers |
| [`SQLITE_BACKEND.md`](docs/SQLITE_BACKEND.md) | reference sink |
| [`REACTIONS_AND_REFERENCES.md`](docs/REACTIONS_AND_REFERENCES.md) | reaction bytes and refs |
| [`SECURITY.md`](docs/SECURITY.md) | trust boundary |
| [`DOMAIN_PACKS.md`](docs/DOMAIN_PACKS.md) | Signals, Splot, Takt vocabulary |
| [`FALA_ARCHITECTURE_STATUS.md`](docs/FALA_ARCHITECTURE_STATUS.md) | current map |

[`CHANGELOG.md`](CHANGELOG.md) is release history.

## License

MIT — see [`LICENSE`](LICENSE).
