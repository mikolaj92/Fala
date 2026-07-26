# Fala

**Version 0.7.13** — exclusive Mojo product; peer conduction + thin core + POSIX host.

## Observable Unix pipe for processes

Fala is a sophisticated **Unix pipe `|` operator for processes**, acting as a mediator of relations between autonomous organs. It does not run a centralized workflow engine or interpret domain payloads; rather, it conducts information impulses along defined contract pathways.

1. **Write small organs** (single-purpose tools) with clear I/O — preferably argv + files/JSON.
2. **Declare contracts** (TOML package topology) defining how signals flow between autonomous processes.
3. **Drive one journal** to idle: claim → execute → complete, and recover state from the event stream. Nested work gets its own journal.

```text
you compose:   package → impulse → run_until_idle → journal
Fala mediates: contract conduction + JournalPort + process host
organs live:   native_function | subprocess (Splot, …) | homeostat gates
```

**Fala is a fully Mojo runtime.**

## Thin Python host binding (optional, memory path)

```python
import fala
result = fala.host_drive(
    run_id="run1",
    impulse={"id": "imp1", "type": "case", "payload": {"n": 1}},
    path={"id": "chain", "effectors": [
        {"id": "root", "capability": "source"},
        {"id": "leaf", "capability": "sink", "conduction": ["root"]},
    ]},
    outputs={"root": {"value": 42}, "leaf": {"done": True}},
)
```

Requires Mojo toolchain. Also: `fala.sdk` (pure-Python effector helpers),
`fala.open_sqlite(path)` / `fala.host_run_package(...)` durable journal path
(optional SQLite sink via sqlite.fire; first use auto-runs
`make -C vendor/sqlite.fire/native` when the shared library is missing — needs a
C compiler + libsqlite3; set `FALA_SKIP_NATIVE_BUILD=1` to skip). Memory path
does not need sqlite.fire. No CPython RuntimeBackendService. Full multi-organ
CLI remains primary for complex ops.

 There is no CPython engine, no YAML packages,
and no multi-runtime fleet.

It is an embedded, **event-first** correlator: impulses move through
correlation paths of effectors; a journal records durable process state; a
**process host** runs OS children (Darwin and Linux). One engine — organ +
journal + host.

```text
Impulse / package (TOML)
        │
        ▼
  CorrelationPath (effectors)
        │
        ├── native_function   (in-process Mojo registry)
        ├── subprocess        (OS child + FALA_EFFECTOR_*)
        └── manual_homeostat  (wait for operator; rearm + EV regulation available)
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
| Multi-organ composition | Signals vocabulary + Splot subprocess (`examples/multi-organ/`) |
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

- **Cybernetic** — Autonomy has reactions, contracts have relationships, and Fala has conduction. Impulses flow through topographies of named contracts. Process execution itself is a local autonomous concern; Fala mediately records and registers the memory trace (Associations, Reactions, and Homeostats) rather than orchestrating the business decision logic.
- **Unix** — A sophisticated multi-process pipe `|` operator. Fala provides a supervisor, process host, and event-stream emitter where durability is a Journal port. SQLite is the reference sink, not product identity. Each nested or parallel system gets its own isolated journal.

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
# Optional: prebuild sqlite.fire for durable smokes (also auto-built on first
# Python open_sqlite / host_run_package when the dylib/so is missing — #106).
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

### Peer conduction (not a workflow tyrant)

Fala mediates named contracts; it does not cancel dependents when an upstream
fails. Terminal upstreams (`succeeded` / `failed` / `cancelled` / `timed_out`)
conduct their payload into the next effector. The child organ decides what the
failure means. One process plan per effector per run remains the durable unit
of activation; feedback cycles wait with a typed diagnosis instead of being
banned by a central scheduler.

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
