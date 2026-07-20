# Fala

**Version 0.3.0** — first **exclusive Mojo** product release.

**Fala is a fully Mojo runtime.** There is no CPython engine, no YAML packages,
and no multi-runtime fleet.

It is an embedded, **event-first** correlator: impulses move through
correlation paths of effectors; a journal records durable process state; a
**process host** runs OS children. One engine — organ + journal + host.

```text
Impulse / package (TOML)
        │
        ▼
  CorrelationPath (effectors)
        │
        ├── native_function   (in-process Mojo registry)
        ├── subprocess        (OS child + FALA_EFFECTOR_*)
        └── manual_homeostat  (wait for operator)
        │
        ▼
  JournalPort  →  InMemory | SQLite | JSONL | Tee
        │
        ▼
  events · associations · reactions · processes · projections
```

## What it is for

| Use | Example |
| --- | --- |
| Local correlation paths | ingest → enrich → export as durable processes |
| Host other Mojo tools | run Splot (or any argv child) as a subprocess effector |
| Observable runs | journaled leases, retries, homeostats, projections |
| Embedded / CLI | drive one run to idle without a web stack |

## What it is not

- Not a Python package or CPython host  
- Not YAML-based packaging (TOML / JSON only)  
- Not a fleet / multi-runtime peer mesh (`fala_runtime` removed)  
- Not Redis/Postgres/Kafka — local journal + filesystem reactions by default  

## Fully Mojo

| | |
| --- | --- |
| Language | **Mojo only** (`mojo/fala/`) |
| Packages | **TOML** (or canonical JSON) |
| Adapters | `subprocess` · `native_function` · `manual_homeostat` |
| Proof | Mojo smokes (`mojo/smoke/` + `pixi.toml`) |
| Python / YAML | **none** in the product tree |

```text
mojo/fala/      engine (organ, journal, driver, host, packages, CLI)
mojo/smoke/     gates
examples/       TOML packages (basic, splot vocabulary, splot-integration)
docs/           architecture & contracts
vendor/         EmberJson, sqlite.fire
tools/          mojo_sql_run.sh, native smokes
```

## Disciplines

- **Cybernetic** — Impulses through `CorrelationPath`s; Associations, Reactions,
  and Homeostats name memory, footprint, and defensive waits
  (Mazur/Kossecki lexicon).
- **Unix** — supervisor + **event-stream** emitter; durability is a Journal
  port with pluggable sinks. SQLite is the **reference sink**, not product
  identity. Nested work gets a **separate journal**, not a shared DB lock.

## Adapters

| Kind | Role |
| --- | --- |
| `subprocess` | argv child; work dir with `input/` + `output/result.json` |
| `native_function` | in-process Mojo callable via registry |
| `manual_homeostat` | open a durable wait for operator input |

Removed: `python_function`, `fala_runtime`.

Details: [`docs/ADAPTER_CONTRACTS.md`](docs/ADAPTER_CONTRACTS.md).

## Quick proof

Requires Pixi/Mojo (see `pixi.toml`):

```bash
git submodule update --init --recursive
make -C vendor/sqlite.fire/native
mise exec -- pixi install
mise exec -- pixi run full-smoke
mise exec -- pixi run extended-smoke
```

Smaller gates:

```bash
mise exec -- pixi run core-smoke    # no SQLite
mise exec -- pixi run host-smoke    # process host + subprocess
```

## Examples

| Path | What |
| --- | --- |
| `examples/correlation-paths/basic/` | TOML package, `native_function` effectors |
| `examples/domain-packs/splot/` | Splot **vocabulary** on Fala records |
| `examples/splot-integration/` | Host sibling **Splot 0.3.1+** as subprocess |

```bash
mise exec -- pixi run example-basic-native
mise exec -- pixi run splot-domain
# sibling checkout ~/…/Splot (or SPLOT_ROOT):
mise exec -- pixi run splot-integration
```

Fala does **not** import Splot. Splot is an optional child product; Fala owns
scheduling and the journal.

## Execution model

`run_until_idle` drives processes **sequentially** (claim → execute → complete).
Fala owns journaled process state and leases; it does not isolate concurrent
multi-run hosts for you. Parallel drivers must keep process ids / work roots
unique. See [`docs/PROCESS_RUNTIME.md`](docs/PROCESS_RUNTIME.md).

## Docs

| Doc | Topic |
| --- | --- |
| [`docs/FALA_ARCHITECTURE_STATUS.md`](docs/FALA_ARCHITECTURE_STATUS.md) | product status |
| [`docs/UNIX_AND_CYBERNETICS.md`](docs/UNIX_AND_CYBERNETICS.md) | philosophy |
| [`docs/ADAPTER_CONTRACTS.md`](docs/ADAPTER_CONTRACTS.md) | effector I/O |
| [`docs/SPLOT_DOMAIN_PACK.md`](docs/SPLOT_DOMAIN_PACK.md) | Splot vocabulary + host |
| [`CHANGELOG.md`](CHANGELOG.md) | releases |

## License

MIT — see [`LICENSE`](LICENSE).
