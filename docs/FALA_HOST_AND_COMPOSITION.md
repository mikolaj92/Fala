# Fala host and composition — product boundary

**Status:** product decision (2026-07).

**Single engine: Mojo.** There is no dual-runtime product and no CPython
engine tree. The optional `python/fala` package is a thin JSON host binding to
the Mojo engine, not another journal, driver, or runtime implementation.

This document separates **what every Fala is** from the historical fleet
machinery once associated with `fala_runtime`.

---

## Thesis

1. **Each Fala is a separate being.** One organ, one journal, one claim loop.
   Falas do **not** need a shared identity graph or mutual discovery.
2. **Fala must be able to run children.** Without a local process host
   (subprocess / equivalent), the product is only an in-process callback
   engine — incomplete as a Unix-shaped correlator.
3. **Historical `fala_runtime` combined two different jobs.** The current
   product keeps local hosting and effector execution, while multi-runtime
   pools and fleet selection are removed from Fala identity.

Nested autonomy does **not** require that parent and child “know” each other
as peers in a pool. It requires **separate journals** and an **explicit
handoff** (process boundary + optional envelope import), same as Unix pipes.

---

## Web and platform UI boundary

Fala is a headless Mojo correlator and local process host. It does not expose
an HTTP application, authentication/session/account/admin pages, templates, or
static frontend assets. The optional `python/fala` package is a JSON host
binding, not a web app factory.

Fala is therefore not a platform COMPAT web host: `product_shell`, Basecoat,
HTMX, Alpine, `/static/platform/` assets, and platform auth/user-management
pins have no attachment point in this repository. Adding a frontend stack
would create a second product surface rather than align an existing one. Any
future web host should adopt the platform shell and assets at that host's
boundary instead of adding chrome to Fala.

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
| **Adapter kinds (local)** | `native_function`, `subprocess`, `manual_homeostat` |
| **Reaction store** | bytes outside the journal; metadata/refs inside |
| **CLI (core)** | implemented `init`, run create/lifecycle/list/inspect/observe, and event/domain inspection on one journal (`--db`); ops retention/bridge/rebuild remain separate |

The native CLI's `run_until_idle` name is reserved for the embedded/library API;
it is not a standalone command. CLI mutations use explicit `--db`, `--run-id`,
and `--now` flags where required. The `schema fala-package` name is reserved
for the native boundary; its schema encoder is not implemented here.

**Not** Essential Fala (optional ops): journal retention / maintain, reaction GC,
bridge outbox/inbox, heavy projection rebuild (`ops_maintenance`, `ops_bridge`,
`ops_projections`). Multi-Fala default remains **separate journals** + subprocess
handoff; bridge is an optional envelope aid.

### Process host is core product

The native process host targets **Darwin and Linux** (shared library
`libfala_process_host.dylib` / `.so`). Windows is out of scope. Build is wired
through `tools/mojo_sql_run.sh` when host smokes run.

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
   own database path. The reserved native `run-until-idle` boundary is not a
   currently executable standalone CLI command.
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
Mojo engine. `host_drive` / `host_drive_json` and `open_memory` drive the memory
path; `open_sqlite`, `host_run_package`, and `delete_terminal_run` cross a JSON
boundary into the native Mojo extension for durable hosting. `MemoryHost` is a
small builder around the memory path. None of these APIs creates a CPython
engine or restores the removed `python_function` adapter.

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

## Historical `fala_runtime` — what to keep vs peel

| Concern today under `fala_runtime` | Destination |
| --- | --- |
| Driver must not hard-require Correlator/`sqlite://` | **Fala** (host/driver hygiene) |
| Enqueue “work elsewhere” as a process that waits | **Re-express as local patterns** first: subprocess to another `fala` CLI, or `waiting` + external complete |
| Bridge outbox/inbox rows on **this** journal | **Optional local surface** (export/import envelopes for one run) — not peer mesh |
| Foreign runtime/run identifiers in imported envelopes | Keep as opaque envelope data; not a live directory of peers |
| `RuntimePool`, fleet policies, `fala_runtime` adapter | **Removed** from the product surface |
| Network peer mesh | **Out of Fala** |

| Old name | Status |
| --- | --- |
| adapter `fala_runtime` | **removed** — use `subprocess` + separate journal |
| RuntimePool / create-pool CLI | **removed** |
| process host | **core local boundary** |

---

## Multi-runtime — removed from product

**Removed from Fala product surface** (adapter, driver, CLI, public exports):

- adapter kind `fala_runtime`
- `RuntimePool` / `DelegationPolicy` operator APIs
- pool policies (`least_busy`, `round_robin`, …)
- `enqueue_fala_runtime_process` / fleet selection

Existing databases may physically retain historical `runtime_pools` and
`delegation_policies` tables. Current code ignores those tables; fresh schemas
do not create them, and neither the tables nor their index are exposed by the
CLI. They are not Fala identity or an active fleet surface.

Nested composition is only:

```text
subprocess (or CLI) → child process → separate journal
optional bridge file / local two-path deliver when the operator chooses paths
```

Each Fala is complete alone. No peer mesh.

---

## Bridge: keep the thin meaning

Bridge stays useful as **explicit envelope handoff** between two journals the
operator (or parent process) already chose:

| Mode | Core? | Role |
| --- | --- | --- |
| Network multi-hop / pools | out of Fala |

v1 semantic merge remains: validate envelope, budgets if present, **no raw
StateFact injection** from child into parent privileged tables.

---

## Optional Nostr transport

Nostr and Fala overlap mechanically at the envelope boundary: both preserve
typed messages, identifiers, causal references, signatures/provenance, and
fan-out to independently acting receivers. Their cybernetic roles differ.
Nostr answers who published a statement and how relays distribute it; Fala
decides what an accepted impulse means inside one local conduction topology,
which capability may react, how that reaction is materialized, and how the
correlator's journal continues.

A Nostr integration is therefore an optional signed transport for bridge
envelopes, not Fala's identity, journal, claim/lease protocol, scheduler, or
delivery guarantee. Relay acceptance does not mean execution, and duplicate or
out-of-order delivery must cross the same validated, idempotent bridge boundary
as file import. Public envelopes select declared capabilities; they do not carry
arbitrary `argv`, `cwd`, environment values, or secrets. Large reactions remain
out of band and travel as canonical references, digests, and metadata.

This boundary keeps each Fala complete and locally ordered. It does not add a
required peer mesh, relay discovery, global journal, marketplace, payment
layer, or exactly-once claim.

---

## Current implementation status

| Surface | Current status |
| --- | --- |
| Event stream and Journal sinks | Implemented: InMemory and reference SQLite; Jsonl/Tee are available sinks |
| Driver and `native_function` | Implemented |
| Separate-journal child composition | Supported through subprocess handoff and explicit bridge envelopes |
| RuntimePool / `fala_runtime` / fleet selection | Removed from the product surface |

## Adapter kinds (product)

| Kind | Status |
| --- | --- |
| `subprocess` | **core** |
| `native_function` | **core** (Mojo registry) |
| `manual_homeostat` | **core** |
| `python_function` | **removed** |
| `fala_runtime` | **removed** |

`runtime_ref` is not an adapter field. A `runtime_ref` key in a manifest or
adapter JSON is an unknown field; the migration layer retains the explicit
historical `fala_runtime` rejection instead of reviving that surface.


---

## Related docs

- [`UNIX_AND_CYBERNETICS.md`](UNIX_AND_CYBERNETICS.md) — recursion without shared DB
- [`PROCESS_RUNTIME.md`](PROCESS_RUNTIME.md) — process/host boundary
- [`ADAPTER_CONTRACTS.md`](ADAPTER_CONTRACTS.md) — effector I/O contract
- [`EVENTS_AND_REPLAY.md`](EVENTS_AND_REPLAY.md) — implemented event inspection and projection replay
- [`JOURNALPORT_CORE_PATH.md`](JOURNALPORT_CORE_PATH.md) — JournalPort durability boundary
