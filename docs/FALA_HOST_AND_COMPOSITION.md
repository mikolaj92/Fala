# Fala host vs multi-runtime — product boundary

**Status:** product decision (2026-07).

**Single engine: Mojo.** There is no dual-runtime product and no CPython
engine tree. Process host, journal, driver, CLI: **Mojo only**.

This document also separates **what every Fala is** from **removed** fleet
machinery that historically lived under the name `fala_runtime`.

---

## Thesis

1. **Each Fala is a separate being.** One organ, one journal, one claim loop.
   Falas do **not** need a shared identity graph or mutual discovery.
2. **Fala must be able to run children.** Without a local process host
   (subprocess / equivalent), the product is only an in-process callback
   engine — incomplete as a Unix-shaped correlator.
3. **`fala_runtime` mixed two different jobs.** We unbundle them:
   - **into Fala:** local host + effector execution boundary
   - **out of Fala identity:** multi-runtime pools / fleet selection

Nested autonomy does **not** require that parent and child “know” each other
as peers in a pool. It requires **separate journals** and an **explicit
handoff** (process boundary + optional envelope import), same as Unix pipes.

---

## Essential Fala (one being)

These are merge-gate for any native land that claims “Fala works.”

| Piece | Role |
| --- | --- |
| **Organ** | Impulse ontology, correlation path pure policy, process state machine |
| **Journal** | `append_batch` / `claim_next` / load; InMemory + reference SQLite (+ Jsonl/Tee as sinks) |
| **Driver** | claim → execute adapter → complete / fail / wait / retry |
| **Host** | spawn and supervise **local** effectors: argv, cwd, env, timeout, stdin/out files |
| **Adapter kinds (local)** | `native_function`, `subprocess`, `manual_homeostat` |
| **Reaction store** | bytes outside the journal; metadata/refs inside |
| **CLI** | operator surface for one journal (`--journal` / `--db`) |

### Process host is core product

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

A nested correlator is still **one Fala process** with **its own journal**:

```text
Parent Fala  (journal J_p)
  │
  └─ subprocess effector
        command: ["fala", "run", "--db", "child.sqlite", …]
              │
              ▼
        Child Fala  (journal J_c, J_c ≠ J_p)
              │
              └─ result via result.json / stdout / bridge file
                    (parent never writes J_c)
```

No `RuntimePool`. No mutual registry. Address of the child is the **command
line and paths the parent chose**, not a fleet membership card.

Optional later: parent imports a **bridge envelope file** the child exported.
That is operator/parent orchestration, not “Falas discover each other.”

---

## Historical `fala_runtime` — what to keep vs peel

| Concern today under `fala_runtime` | Destination |
| --- | --- |
| Driver must not hard-require Correlator/`sqlite://` | **Fala** (host/driver hygiene) |
| Enqueue “work elsewhere” as a process that waits | **Re-express as local patterns** first: subprocess to another `fala` CLI, or `waiting` + external complete |
| Bridge outbox/inbox rows on **this** journal | **Optional local surface** (export/import envelopes for one run) — not peer mesh |
| `RuntimeRef` / `RunRef` as typed ids in envelopes | Keep as **envelope fields** when importing foreign ids; not a live directory of peers |
| `RuntimePool`, fleet policies, `fala_runtime` adapter | **Removed** from product surface |
| Network peer mesh | **Out of Fala** |

| Old name | Status |
| --- | --- |
| adapter `fala_runtime` | **removed** — use `subprocess` + separate journal |
| RuntimePool / create-pool CLI | **removed** |
| process host | **merge-gate** for Mojo land |

---

## Multi-runtime — removed from product

**Removed from Fala product surface** (adapter, driver, CLI, public exports):

- adapter kind `fala_runtime`
- `RuntimePool` / `DelegationPolicy` operator APIs
- pool policies (`least_busy`, `round_robin`, …)
- `enqueue_fala_runtime_process` / fleet selection

Historical storage rows/tables may still exist in SQLite schema for
compatibility; they are **not** part of Fala identity and must not be
re-exposed as core features.

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
| Local file export/import | optional helper | Unix-friendly merge without shared DB |
| Local two-path deliver (same machine) | optional helper | convenience for tests/ops |
| Network multi-hop / pools | optional multi-runtime | not default |

v1 semantic merge remains: validate envelope, budgets if present, **no raw
StateFact injection** from child into parent privileged tables.

---

## Mojo land checklist (revised)

| Gate | Item |
| --- | --- |
| **Must** | Event-stream core + Journal sinks (memory/sqlite/jsonl/tee) |
| **Must** | Driver + `native_function` path |
| **Must** | **Process host + `subprocess` path green** (children of Fala) |
| **Removed** | RuntimePool / `fala_runtime` / fleet — do not port |
| **Nice** | Bridge file export/import for nested CLI children |

## Adapter kinds (product)

| Kind | Status |
| --- | --- |
| `subprocess` | **core** |
| `native_function` | **core** (Mojo registry) |
| `manual_homeostat` | **core** |
| `python_function` | **removed** |
| `fala_runtime` | **removed** |

See also [`MOJO_EVENT_STREAM_MIGRATION.md`](MOJO_EVENT_STREAM_MIGRATION.md).

---

## Related docs

- [`UNIX_AND_CYBERNETICS.md`](UNIX_AND_CYBERNETICS.md) — recursion without shared DB
- [`PROCESS_RUNTIME.md`](PROCESS_RUNTIME.md) — process/host boundary
- [`ADAPTER_CONTRACTS.md`](ADAPTER_CONTRACTS.md) — effector I/O contract
- [`MULTI_FALA_COMPOSITION.md`](MULTI_FALA_COMPOSITION.md) — composition without peer mesh
- [`EVENT_STREAM_CORE.md`](EVENT_STREAM_CORE.md) — journal + child journal rules
