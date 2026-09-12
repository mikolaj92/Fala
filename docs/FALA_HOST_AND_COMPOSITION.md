# Fala host and composition — product boundary

**Status:** product decision (2026-07).

**Single engine: Mojo.** The optional `python/fala` package is a thin JSON host
binding to the Mojo engine, not another journal, driver, or runtime
implementation.
The official Mojo 1.0 bridge is `PyInit_*` + `PythonModuleBuilder`;
`ensure_native` stays because `import mojo.importer` cannot pass package
import paths.

This document separates **what every Fala is** from nested composition.
Fleet pools and peer discovery are out of product.

---

## Thesis

1. **Each Fala is a separate being.** One organ, one journal, one claim loop.
   Falas do **not** need a shared identity graph or mutual discovery.
2. **Fala must be able to run children.** Without a local process host
   (subprocess / equivalent), the product is only an in-process callback
   engine — incomplete as a Unix-shaped correlator.
3. **Local hosting is identity; fleet selection is not.** Nested work is a
   subprocess plus a separate child journal, not a pool of peers.

Nested autonomy does **not** require that parent and child “know” each other
as peers in a pool. It requires **separate journals** and an **explicit
handoff** (process boundary + optional envelope import), same as Unix pipes.

---

## Headless product

Fala is a Mojo correlator and local process host. It does not expose an HTTP
application, templates, or frontend assets. The optional `python/fala` package
is a JSON host binding, not a web app.

---

## Essential Fala (one being)

Core Fala consists of the following pieces; a composition is complete when it
can use these local boundaries.

| Piece | Role |
| --- | --- |
| **Organ** | Impulse ontology, correlation path pure policy, process state machine |
| **Journal** | `append_batch` / `claim_next` / load; InMemory + reference SQLite (+ Jsonl/Tee as sinks) |
| **Driver** | claim → execute adapter → complete / fail / wait / retry |
| **Host** | spawn and supervise **local** effectors: argv, cwd, env, timeout, stdin/out files |
| **Adapter kinds (local)** | `native_function`, `subprocess`, `manual_homeostat`, `child_path` |
| **Reaction store** | bytes outside the journal; metadata/refs inside |
| **CLI (core)** | implemented `init`, run create/lifecycle/list/inspect/observe, and event/domain inspection on one journal (`--db`); ops retention/bridge/rebuild remain separate |

`run_until_idle` is the embedded parent loop that sits until children answer,
go silent, or are stopped; it is not a CLI command. CLI mutations use explicit
`--db`, `--run-id`, and `--now` flags where required.

**Not** Essential Fala (optional ops): journal retention / maintain, reaction GC,
bridge outbox/inbox, heavy projection rebuild (`ops_maintenance`, `ops_bridge`,
`ops_projections`). Multi-Fala default remains **separate journals** + subprocess
handoff; bridge is an optional envelope aid.

### Process host is core product

The native process host targets **Darwin and Linux** (shared library
`libfala_process_host.dylib` / `.so`). Windows is out of scope. Build is wired
through `tools/mojo_sql_run.sh` when host smokes run.
Linux builds require glibc 2.29 or newer for
`posix_spawn_file_actions_addchdir_np`.

Effectors are the operational edge of the organ. The default edge is a
**process**:

```text
Fala driver
  → claim process
  → host starts argv (or native_function in-process)
  → child writes output/result.json (subprocess contract)
  → driver commits complete/fail via journal
```

`native_function` proves the organ without OS spawn. It is necessary for
tests and embedded callables. It is **not** a substitute for the host in
product identity: packages, examples, and isolation assume subprocess
boundaries (manifests, redaction, no open parent DB handles).

### Graph authoring CLI

Treat the expanded graph like code before opening a runtime journal:

```bash
fala graph expand --package package.toml
fala graph validate --package package.toml
fala graph fingerprint --package package.toml
fala graph diff --before old.fala-package.toml --after new.fala-package.toml
```

All four commands emit stable JSON. Expansion materializes bounded templates;
fingerprints cover that canonical topology plus contracts and policies; diff
classifies nodes, edges, conditions, terminals, capabilities, adapters, retry,
timeout, and runtime-policy changes. Validation reports source JSON
pointer/TOML paths. These authoring operations only read package files: they do
not open journals or execute adapters.

For a durable run, `fala explain --db state.sqlite --package package.toml
--run-id RUN [--process-id ID | --terminal ID]` returns canonical JSON derived
only from the package graph and committed journal facts. Each process includes
its exact conduction dependencies, statuses, condition source/path and
expected/observed scalar, missing facts, lease, attempts, reason, and related
event IDs. Reasons distinguish `not_declared`, `not_materialized`, `not_ready`,
`condition_not_met`, `upstream_failed`, `waiting`, and `terminal`. Full payloads
and adapter environments are deliberately omitted, so explanations do not
become a secret-exfiltration surface.

Whole-graph acceptance rehearsal uses the normal materialized plan, durable
journal, retry transitions, and correlation advancement while replacing every
adapter boundary with fixture outcomes:

```bash
fala rehearse --package package.toml --fixture acceptance.json --path-id ship \
  --journal rehearsal.sqlite --report rehearsal.json
```

`effectors` in the fixture maps an effector ID (or its capability as fallback)
to an ordered sequence of `{kind = result|failure|timeout|wait, ...}` outcomes.
Its `assert` object may require `terminal`, graph `fingerprint`, per-effector
`attempts`, and `forbidden_effectors`. Missing or malformed fixture data fails
closed; production argv and native functions are never called. The resulting
journal remains available to `explain`, while the canonical report records the
fingerprint, terminal, fixture-only policy, attempts/event order, and status.

Every effector declares a non-empty `output_schema` that names the answer.
`{ type = "object" }` is not a contract. Rehearsal retains that contract.
Payload validation
and typed terminal selection are shared with production; see
[output contracts](OUTPUT_CONTRACTS.md) for the supported schema subset.
Rehearsal fails closed when a finite declared variant has no fixture. A passing
fixture is not proof that an agent told the truth.

### Declarative child paths without multi-runtime

A package may author `adapter.kind = "child_path"`. The loader requires a
`package_ref`, child `path_id`, separate `journal_root`, explicit
`input_mapping` and `terminal_mapping`, positive `lifetime_seconds`, and a
`retention` policy (`keep` or `delete_on_success`). The host compiles this node
to the existing argv subprocess boundary; `child_path` is not a runtime adapter
kind and does not restore `fala_runtime`.

The child run ID and journal filename are a stable digest of the parent
run/process identity. Replaying the same parent process therefore addresses the
same durable child. Its typed `path_result` returns through `result.json`, with
`child_ref = {journal, run_id, path_digest, terminal}` included in parent
values/metadata. A timeout or crash fails the parent process while leaving the
child journal inspectable. `retention = "keep"` leaves that journal on disk.
`delete_on_success` unlinks the child SQLite file (and WAL/SHM sidecars) after
the typed `path_result` is written to the parent; it is not `maintain_journal`.
Neither parent runtime nor child runner writes the other journal.

### Child Fala without multi-runtime

A nested correlator is a **separate Fala process** with its **own journal**:

```text
Parent Fala  (journal J_p)
  │
  └─ subprocess effector
        command: ["<child-program>", "--db", "child.sqlite", …]
              │
              ▼
        Child Fala  (journal J_c, J_c ≠ J_p)
              │
              └─ result via result.json / stdout / bridge file
                    (parent never writes J_c)
```

No `RuntimePool` or mutual registry exists. The address of the child is the
**command line and paths the parent chose**, not a fleet membership card.

Parent/child composition uses separate journals and an explicit handoff:

1. Parent runs a subprocess effector whose declared child program owns its
   own database path.
2. Child never shares the parent journal path.
3. Results return through the subprocess contract (`result.json`) and/or the bridge commands below:
```text
fala bridge list --db JOURNAL.sqlite --run-id RUN_ID
fala bridge deliver --db SOURCE.sqlite --run-id RUN_ID --delivery-id DELIVERY_ID --target-db TARGET.sqlite --now RFC3339
fala bridge export --db JOURNAL.sqlite --run-id RUN_ID --delivery-id DELIVERY_ID --out delivery.json
fala bridge import --db JOURNAL.sqlite --file delivery.json
```

Optional bridge import is operator/parent orchestration, not “Falas discover
each other.”

---

## Optional Python host binding

The wheel ships `python/fala` as a convenience boundary over the authoritative
Mojo engine. `host_drive` and `open_memory` drive the memory
path; `open_sqlite`, `host_run_package`, and `delete_terminal_run` cross a JSON
boundary into the native Mojo extension for durable hosting. `MemoryHost` is a
small builder around the memory path. None of these APIs duplicates the engine.

The `host_run_package` binding uses an empty native-function registry; a package
whose selected path requires `native_function` therefore cannot execute through
this thin host unless a registered registry is supplied by another native
boundary. The binding does not expose manifest adapter metadata through
`fala.sdk` helpers.

`fala.sdk` is different: it helps a Python **subprocess effector** read
`FALA_EFFECTOR_MANIFEST`, inspect declared input/conduction/config and runtime
injections, and write `FALA_EFFECTOR_OUTPUT_DIR/result.json`. It conforms to the
same language-neutral wire contract as any other child process; its helpers do
not expose manifest adapter metadata.

---

## Nested composition

Nested work is only:

```text
subprocess (or CLI) → child process → separate journal
optional bridge file / local two-path deliver when the operator chooses paths
```

Each Fala is complete alone. No peer mesh. Fleet pools, peer discovery, and
network multi-hop delivery are out of product.

## Bridge: keep the thin meaning

Bridge stays useful as **explicit envelope handoff** between two journals the
operator (or parent process) already chose. v1 semantic merge remains: validate
envelope, budgets if present, **no raw StateFact injection** from child into
parent privileged tables.

## Current implementation status

| Surface | Current status |
| --- | --- |
| Event stream and Journal sinks | Implemented: InMemory and reference SQLite; Jsonl/Tee are available sinks |
| Driver and `native_function` | Implemented |
| Separate-journal child composition | Supported through subprocess handoff and explicit bridge envelopes |

## Adapter kinds (product)

| Kind | Status |
| --- | --- |
| `subprocess` | **core** |
| `native_function` | **core** (Mojo registry) |
| `manual_homeostat` | **core** |
| `child_path` | **host compile** to subprocess (`python/fala/child_path.py`) |

Unknown adapter kinds fail closed. `runtime_ref` is not an adapter field.


---

## Related docs

- [`UNIX_AND_CYBERNETICS.md`](UNIX_AND_CYBERNETICS.md) — recursion without shared DB
- [`PROCESS_RUNTIME.md`](PROCESS_RUNTIME.md) — process/host boundary
- [`ADAPTER_CONTRACTS.md`](ADAPTER_CONTRACTS.md) — effector I/O contract
- [`EVENTS_AND_REPLAY.md`](EVENTS_AND_REPLAY.md) — implemented event inspection and projection replay
- [`JOURNALPORT_CORE_PATH.md`](JOURNALPORT_CORE_PATH.md) — JournalPort durability boundary
