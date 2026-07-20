# Migrating Fala 0.2.2 onto Mojo — core first, sinks later

**Audience:** native Mojo work after Python `main` 0.2.2 (event-stream core).

**Strategic order (this is the product decision):**

```text
1. Port CORE to Mojo          ← primary
2. Port SQLite adapter        ← secondary (reference sink)
3. Port other sinks/adapters  ← JSONL, Tee, transports, …
```

**Chosen execution path (indifferent between rebase / rewrite):**  
**Fresh branch `mojo-core-0.2.2` from `main` 0.2.2**, selectively lifting pure
modules from historical `mojo`, then writing JournalPort + InMemory. No full
rebase of the SQLite monolith. Existing `mojo` remains a quarry for the SQLite
adapter phase (`NativeJournal`, `domain_store`, schema, CLI db).

SQLite is **out of the core** in 0.2.2. The Mojo port must not re-glue it into
the engine’s identity.

### Bootstrap status

| Item | Status |
| --- | --- |
| Branch | `mojo-core-0.2.2` ([PR #93](https://github.com/mikolaj92/Fala/pull/93), **draft** until core **and** adapters complete) |
| Lifted pure | `status`, `processes`, `correlation`, `domain`, models, json, toml, validation |
| New core | `journal_port.mojo`, `memory_journal.mojo` |
| Proof | `pixi run core-smoke` (no sqlite.fire) |
| SQLite | not in tree yet (adapter phase) |

### Merge policy

**One land when the whole Mojo port is done** — core **and** adapters in the
same integration branch / PR. Do **not** merge a partial core-only slice to
`main`, and do **not** ship SQLite (or JSONL) as a follow-up merge after core.

Build order inside the branch still matters (core before SQLite code paths),
but **merge is atomic**:

```text
work order:   core → sqlite adapter → other adapters
merge gate:   all of the above proven → one PR ready → merge
```

Journal is **part of core** (Protocol + InMemory). SQLite/JSONL/Tee are
**adapters in the same delivery**, not separate release trains.

#### Work-order checklist (inside the branch)

**Core**

1. JournalPort types + InMemoryJournal — **done**
2. Pure status / processes / correlation — **done** (smokes)
3. Memory-backed mutators: run, impulse, process schedule/claim/complete — **done** (`MemoryRuntime`)
4. Driver against JournalPort (memory) — **done** (`MemoryDriver` + e2e)
5. Package load + native_function registry — **done** (manifest + registry smokes)
6. Facade `open_journal` — **done** (`open_journal.mojo`)
7. CLI surface — **done** (`cli.mojo` + `native_cli_surface`, `cli-smoke`)
8. One end-to-end example on memory — **done** (`core_memory_e2e`)
9. MemoryDriver ↔ NativeFunctionRegistry — **done** (`core_driver_registry`)
10. `core-smoke` green without `sqlite.fire` — **done**

**Adapters (same branch, before merge)**

11. SqliteJournalPort + NativeJournal/schema lift — **done**
12. SQLite create/schedule/claim/complete + migration matrix — **done**
13. JsonlJournal + torn-line smoke — **done**
14. TeeJournal — **done**
15. CLI operator path on SQLite — **done**
16. Full historical matrix (native_cli_semantics breadth, domain_store maintenance, bridge transport host) — partial/open
17. Subprocess / fala_runtime transport hosts as required — open

**Merge when:** full checklist green; draft PR converted to ready once.

**Proof today:** `pixi run full-smoke` (= core-smoke + adapter-smoke) — green.

Companion docs:

- Python design: [`EVENT_STREAM_CORE.md`](EVENT_STREAM_CORE.md)
- Philosophy: [`UNIX_AND_CYBERNETICS.md`](UNIX_AND_CYBERNETICS.md)
- Mojo inventory (branch `mojo`): `docs/FULL_MOJO_PORT_PLAN.md`,
  `docs/NATIVE_PARITY_MATRIX.md`

---

## 0. Priority principle

| Layer | What it is | Mojo priority |
| --- | --- | --- |
| **Core** | Cybernetic organ + pure policy + Journal Protocol + InMemory sink + driver loop against the port | **First** |
| **SQLite adapter** | `SqliteJournal` / `NativeJournal` + schema + domain tables | **Second** |
| **Other adapters** | Jsonl, Tee, subprocess host, `fala_runtime` bridge transport, … | **Later** |

**Core must compile, test, and run a full correlation path with zero SQLite.**  
Only then wire the SQLite sink as an optional/default production adapter.

This matches Python 0.2.2:

- core: models, `runtime_pure`, journal types, `InMemoryJournal`, service/driver
- adapter: `SqliteJournal` → `Correlator`
- later: `JsonlJournal`, `TeeJournal`, network bridges

---

## 1. What “core” means in Mojo terms

### In scope for Core Mojo (port first)

| Python 0.2.2 | Mojo target | Reuse from branch `mojo` |
| --- | --- | --- |
| `runtime_models` / Impulse ontology | `domain.mojo`, `models_native.mojo` | **Reuse** typed structs |
| `runtime_pure` | `status.mojo`, `processes.mojo` | **Reuse** transition/claim pure helpers |
| `correlation_paths` pure planning | `correlation.mojo` | **Reuse** graph/advance pure |
| `journal/types.py` + Protocol | **new** `journal_port.mojo` (types + trait) | — |
| `InMemoryJournal` | **new** `memory_journal.mojo` | pure claim from `processes.mojo` |
| Journal-backed in-memory backend (subset) | **new** map-backed mutators over memory journal | patterns from domain APIs, not SQL |
| Driver claim→execute→complete | `native_driver.mojo` against **JournalPort**, not SQLite | **Reuse** loop; **rebind** store |
| `python_function` → native registry | `adapters.mojo` / `NativeFunctionRegistry` | **Reuse** |
| Package load (TOML/JSON) | `package.mojo`, `toml.mojo` | **Reuse** |
| Facade `from_journal` / `open_journal` | thin Mojo entrypoints | `kind=memory` first |

### Out of core (adapter phases)

| Piece | Phase |
| --- | --- |
| `NativeJournal` + `schema.mojo` + SQL DDL | **SQLite adapter** |
| `domain_store.mojo` SQL CRUD | **SQLite adapter** (or split: pure domain logic stays core, SQL rows stay adapter) |
| `sqlite.fire` linkage | **SQLite adapter** |
| CLI `db migrate/vacuum` against files | **SQLite adapter** (core CLI can run memory paths first) |
| `JsonlJournal` / Tee | **Other sinks** |
| Subprocess process host, bridge network | **Other adapters / transports** |

### Mental model

```text
┌─────────────────────────────────────────────────────────┐
│  CORE (port first, no sqlite.fire required)             │
│                                                         │
│  Impulse ontology · pure status/claim · correlation     │
│  JournalPort trait · InMemoryJournal · driver · CLI     │
│  AutonomousCorrelator-equivalent façade                 │
└────────────────────────────┬────────────────────────────┘
                             │ JournalPort only
           ┌─────────────────┼─────────────────┐
           ▼                 ▼                 ▼
    InMemory (core)   SqliteJournal      Jsonl / Tee
    always available  (adapter #1)       (adapters #2+)
           │                 │
           │                 ▼
           │          NativeJournal + DomainStore
           │          (existing mojo/ code — REHOME)
```

---

## 2. How to treat the existing `mojo` branch

Branch `mojo` built **SQLite-first native**. That work is **not wasted**; it is
mostly the future **SQLite adapter package**.

| Existing module | After reframe |
| --- | --- |
| `journal.mojo` (`NativeJournal`) | Lives under adapter layer; implements `JournalPort` |
| `domain_store.mojo` | SQLite-backed domain materialization for that adapter |
| `schema.mojo` | SQLite migrations for that adapter |
| `status.mojo` / `processes.mojo` / `correlation.mojo` | **Promote to core** (already pure enough) |
| `native_driver.mojo` | Core; depend on `JournalPort`, not `Connection` |
| Pixi smokes that need `.sqlite` | Move to adapter test suite; core smokes use memory |

**Do not** make “green on SQLite file” the gate for core port completion.  
**Do** keep those smokes as the gate for the SQLite adapter phase.

---

## 3. Phased plan

### Phase C0 — tree / rebase hygiene

- Rebase or merge `main` 0.2.2 docs + oracle snapshot strategy.
- Document boundary: `mojo/fala/core/` vs `mojo/fala/adapters/sqlite/` (or
  equivalent naming). Exact directory split can be gradual.
- Refresh `reference/fala` from Python 0.2.2 when differential needs it.

### Phase C1 — Core: pure organ (no journal trait yet)

**Goal:** cybernetic + pure policy compile and unit-smoke without SQLite.

- Keep/trim: `status`, `processes`, `correlation` (pure only).
- Smoke: transition matrix, claim eligibility, correlation readiness/fixed-point.
- **Gate:** no link to `sqlite.fire` required for this smoke bundle.

### Phase C2 — Core: JournalPort + InMemoryJournal

**Goal:** event-stream core on Mojo.

- Port batch types + trait from Python `fala/journal`.
- Implement `InMemoryJournal` (append_batch, claim_next, load, replay).
- Minimal map-backed backend: create_run, accept_impulse, schedule, claim,
  complete, list_events (enough to drive one correlation path).
- **Gate:** end-to-end path in memory — instantiate path → run_until_idle →
  events present; reopen from memory snapshot/load if applicable.

### Phase C3 — Core: driver + native_function + package

**Goal:** real work without SQLite.

- Bind `native_driver` to JournalPort/memory backend.
- Package TOML + `NativeFunctionRegistry` effectors.
- CLI subset: create-run, run-until-idle, events list against memory journal.
- **Gate:** basic example (ingest/enrich/export or equivalent) fully native,
  memory-only.

### Phase S1 — SQLite adapter (first production sink)

**Goal:** rehome existing `NativeJournal` / `domain_store` / `schema` as
`JournalPort` implementation.

- Thin wrap: leading unit → existing TX methods (same rule as Python PR4).
- `runtime_uri = sqlite://…`
- CLI `--journal` / `--db` for file path.
- Move SQLite smokes here; prove reopen/crash/migration.
- **Gate:** parity matrix rows for persistence; default local product path can
  select SQLite adapter without core importing SQL.

### Phase S2+ — Other adapters

| Order | Adapter | Notes |
| --- | --- | --- |
| S2 | JsonlJournal | fsync-before-index; reuses EmberJson |
| S3 | TeeJournal | primary + secondaries |
| S4 | Subprocess transport | process host already started on `mojo` |
| S5 | `fala_runtime` / bridge transport | persistence exists; host may still be unavailable |
| S6 | stream.merged helpers | pure; export/debug only |

---

## 4. What to reuse vs rewrite

### Reuse as core (high confidence)

- Pure transition / claim / retry policy (`status.mojo`, `processes.mojo`)
- Pure correlation planning and advance (`correlation.mojo`)
- Typed domain models (`domain.mojo`, `models_native.mojo`)
- Adapter registry pattern, package/TOML loaders
- Driver loop structure (rebind storage interface)
- Pixi/toolchain, EmberJson for JSON payloads

### Reuse as SQLite adapter (high confidence)

- Entire `NativeJournal` TX engine
- `domain_store` SQL mutators
- `schema` migrations and doctor/db CLI bits
- Existing process/claim SQL smokes
- `sqlite.fire` vendor stack

### Write new (core)

- JournalPort trait + batch types
- InMemoryJournal + memory materialization
- Core façade that never imports SQLite
- Core-only smoke entrypoints (no `.sqlite` path)

### Write new (later adapters)

- Jsonl / Tee
- Any transport hosts still marked unavailable

---

## 5. Success criteria by milestone

### Core Mojo “done enough”

- [ ] Core package builds without `sqlite.fire`
- [ ] InMemory journal drives claim/complete + correlation advance
- [ ] At least one full example path green in memory
- [ ] Public story: “native core is event-first”; SQLite not required to *be* Fala

### SQLite adapter “done enough”

- [ ] Implements JournalPort over existing NativeJournal
- [ ] Schema migrate/reopen/crash smokes green
- [ ] Default operator path can use SQLite file via `--journal`/`--db`
- [ ] No core module imports Connection/SQL

### Platform

- [ ] Docs on mojo tree match 0.2.2 language (Unix + cybernetics)
- [ ] Parity matrix distinguishes core vs adapter evidence

---

## 6. Anti-patterns (avoid)

1. **Making SQLite the compile-time dependency of core** — regresses 0.2.2.
2. **Rewriting NativeJournal before core JournalPort exists** — wrong order.
3. **Blocking core on full 71-method SQL parity** — adapter breadth is S1+.
4. **Driver talking to `Connection` directly** — must talk to JournalPort.
5. **Treating Python `reference/` pre-0.2.2 as oracle for event-stream** —
   refresh when claiming 0.2.2 parity.

---

## 7. Suggested first PRs (after rebase onto main)

| PR | Title | Layer |
| --- | --- | --- |
| 1 | `feat(mojo-core): JournalPort types + trait` | Core |
| 2 | `feat(mojo-core): InMemoryJournal + memory path smoke` | Core |
| 3 | `feat(mojo-core): driver against JournalPort (memory)` | Core |
| 4 | `feat(mojo-sqlite): SqliteJournalPort wrap NativeJournal` | Adapter |
| 5 | `feat(mojo-sqlite): CLI --journal + db smokes rehomed` | Adapter |
| 6+ | Jsonl, Tee, transports | Other |

PR1–3 must not require a database file. PR4 is the first allowed SQL link.

---

## 8. One-line summary

**Port the event-stream core (cybernetics + JournalPort + memory) to Mojo
first; treat existing native SQLite work as the first adapter, then JSONL and
the rest — the same layering Python 0.2.2 already proved.**
