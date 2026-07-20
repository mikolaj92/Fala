# Migrating Fala 0.2.2 event-stream architecture onto the Mojo port

**Audience:** native Mojo work on branch `mojo` (tip `4974d76`) after Python
`main` 0.2.2 (`8a5cc5f`).

**Goal:** keep everything already proven on Mojo, then layer the **event-first
Journal port** (Unix sinks) on top of the existing cybernetic/SQLite organ —
without a second full rewrite.

Companion docs:

- Python design (landed): [`EVENT_STREAM_CORE.md`](EVENT_STREAM_CORE.md)
- Philosophy: [`UNIX_AND_CYBERNETICS.md`](UNIX_AND_CYBERNETICS.md)
- Mojo port plan (on `mojo` branch): `docs/FULL_MOJO_PORT_PLAN.md`
- Parity matrix (on `mojo` branch): `docs/NATIVE_PARITY_MATRIX.md`

---

## 1. Where the two trees stand

| Axis | `main` 0.2.2 (Python) | `mojo` branch (native) |
| --- | --- | --- |
| Product shape | Event-first core; SQLite = **reference sink** | Native Mojo runtime; SQLite = **the store** |
| Oracle | Live package under `src/fala` | Frozen under `reference/fala` (pre–event-stream) |
| Durability API | `Journal` Protocol + sinks | `NativeJournal` **is** SQLite TX API |
| Pure policy | `runtime_pure.py` | `status.mojo`, `processes.mojo` |
| Domain CRUD | `RuntimeBackend` / JournalBackedBackend | `NativeDomainStore` |
| Correlation | `correlation_paths.py` | `correlation*.mojo` + persistence |
| Driver / adapters | Python + subprocess | Native host + typed unavailable transports |
| Proof | unittest + conformance | Pixi smokes + bounded differential oracle |

**Merge-base** is pre–event-stream (`faa0015`). Mojo never saw:

- `fala.journal` (types, memory, sqlite wrap, jsonl, tee, stream)
- `runtime_pure` as a named module (logic exists natively elsewhere)
- `from_journal` / `JournalConfig` / CLI `--journal`
- `runtime_models` split
- docs `UNIX_AND_CYBERNETICS.md` / 0.2.2 packaging

Mojo **already has** most of what 0.2.2 *preserves* under the SQLite sink:

- atomic command + event + state
- idempotent append / replay
- `claim_next_ready` with lease reaps
- process transition matrix
- correlation advance + pure planning
- schema v6, bridge records, pools/policies (bounded)
- CLI `db` surface, smokes, clean-install path

---

## 2. Conceptual alignment (do not fight either design)

```text
                    ┌──────────────────────────────────────┐
                    │  Cybernetic organ (unchanged intent) │
                    │  Impulse · CorrelationPath · Process │
                    │  Association · Reaction · Homeostat  │
                    └──────────────────┬───────────────────┘
                                       │ pure policy + mutators
                                       ▼
                    ┌──────────────────────────────────────┐
                    │  Journal port (0.2.2 addition)       │
                    │  append_batch / claim_next / load     │
                    └──────────────────┬───────────────────┘
                         ┌─────────────┼─────────────┐
                         ▼             ▼             ▼
                   InMemory*     SqliteJournal*   Jsonl*
                         │             │
                         │             ▼
                         │     NativeJournal (KEEP)
                         │     + DomainStore (KEEP)
                         │     + schema (KEEP)
```

\* New or thin wrappers on Mojo.  
**Keep** = reuse current `mojo/fala/journal.mojo`, `domain_store.mojo`, `schema.mojo`.

**Rule:** `NativeJournal` becomes the **SQLite sink implementation**, not the
product identity. Cybernetic names stay; Unix stream boundary is added above
the existing TX engine.

---

## 3. What to reuse as-is (high confidence)

| Mojo asset | Maps to 0.2.2 | Notes |
| --- | --- | --- |
| `mojo/fala/journal.mojo` (`NativeJournal`) | `SqliteJournal` + `Correlator` TX core | Rename in docs only; wrap with Journal Protocol |
| `domain_store.mojo` | domain `put_*` / record paths | Stay SQLite authority for tables |
| `schema.mojo` | schema v6 / migrations | Unchanged |
| `status.mojo` | run/process transition predicates | Align with `runtime_pure` matrices |
| `processes.mojo` (`process_is_claimable`, claim, retry, expire) | `runtime_pure` claim helpers | Already pure; keep as source of truth natively |
| `correlation*.mojo` | `correlation_paths` pure + advance | Keep; wire through journal mutators |
| `sqlite.mojo` + `vendor/sqlite.fire` | SQLite driver | Keep |
| `json.mojo` + EmberJson | JSONL / payloads | Reuse for Jsonl sink |
| `native_driver.mojo`, adapters, process host | driver / effectors | Keep; only source URI / multi-journal later |
| `cli.mojo` | CLI | Add `--journal` alias; keep `--db` |
| Pixi smokes + parity matrix | proof system | Extend matrix rows for Journal port |
| `reference/fala` layout idea | oracle | **Refresh oracle** from main 0.2.2 (see §6) |

---

## 4. What must be added or reshaped

### 4.1 Journal Protocol types (new, small)

Port Python `fala/journal/types.py` → e.g. `mojo/fala/journal_port.mojo` (name
avoids clash with existing `journal.mojo`):

- `StateFact`, `CommandUnit`, `JournalBatch`, `AppendResult`
- `ClaimRequest`, `ClaimResult`
- helpers: leading command / leading idempotency key

Wire format for JSONL can match Python:

```text
{"v":1,"kind":"journal_batch","batch":{...}}
```

### 4.2 Trait / protocol for sinks

```text
trait JournalPort:
  fn runtime_uri(self) -> String
  fn append_batch(mut self, batch: JournalBatch) raises -> AppendResult
  fn claim_next(mut self, request: ClaimRequest) raises -> ClaimResult
  fn get_command_by_idempotency(...)
  fn list_events(...)
  fn load(...)
```

Mojo naming: prefer `JournalPort` / `NativeJournalPort` so `NativeJournal`
remains the SQLite engine type.

### 4.3 Sinks

| Sink | Strategy |
| --- | --- |
| **SqliteJournalPort** | Thin adapter: leading unit → existing `NativeJournal` methods (`create_run`, `transition_process`, `claim_next_ready`, …). Non-leading units ignored as inputs (Correlator regenerates side effects) — same rule as Python PR4. |
| **InMemoryJournalPort** | New module; reuses `processes.mojo` pure claim policy; maps under one lock. Critical for tests and nested Fala without file locks. |
| **JsonlJournalPort** | New; durable line + fsync then index (port Python barrier order). Index can be InMemory. |
| **TeeJournalPort** | Primary + secondaries; claim on primary only. |

### 4.4 Backend façade

Python `JournalBackedBackend`:

- SQLite → delegate to correlator  
- Memory/JSONL → map-backed backend + journal batches  

Mojo equivalent:

- SQLite path: keep `NativeDomainStore` + `NativeJournal` as today, optionally
  behind `JournalBacked` façade that exposes `runtime_uri`.
- Memory/JSONL: implement only the subset needed for smokes first (run create,
  accept impulse, schedule/claim/complete, list events), then grow toward
  parity matrix — **do not block** SQLite production path on full memory
  conformance.

### 4.5 Constructors / CLI

- `open_journal(kind, path)` / `from_journal` on the native façade
- CLI: `--journal` preferred, `--db` alias (mirror Python 0.2.2)
- Package config: optional `runtime.journal` alongside `backend`

### 4.6 Stream composition (low priority)

Port `journal/stream.py` helpers (`nest_child_batch`, `stream_merged_envelope`)
as pure functions — no SQLite. Use for export/debug; v1 multi-Fala semantic
merge stays bridge (already partially on Mojo).

### 4.7 Docs on mojo branch

- Import / link `UNIX_AND_CYBERNETICS.md` and this file after rebase.
- Update `FULL_MOJO_PORT_PLAN.md` “SQLite-only production” language →
  event-first core, SQLite reference sink.
- Add NATIVE_PARITY_MATRIX rows for Journal port + memory/jsonl smokes.

---

## 5. Recommended execution sequence (reuse-first)

Do **not** rewrite `NativeJournal` from scratch. Sequence mirrors Python
PR1–PR10 but skips work already done natively.

```text
M0  Rebase/merge strategy (§6)
M1  JournalPort types + trait + smokes (no production call sites)
M2  SqliteJournalPort wrap over NativeJournal (leading-unit dispatch)
M3  InMemoryJournalPort + claim pure helpers from processes.mojo
M4  Façade runtime_uri + open_journal; CLI --journal alias
M5  JsonlJournalPort + fsync barrier smokes (+ optional Tee)
M6  stream.merged helpers (pure)
M7  Expand memory backend toward matrix; nested child journal smoke
M8  Refresh reference oracle from main 0.2.2; extend differential
```

### Phase detail

**M1 — types only**  
Port structs + serialize/deserialize batch to JSON via EmberJson. Smoke:
round-trip batch, multi-unit claim shape, leading-key helper.

**M2 — SQLite wrap**  
```text
append_batch(leading=run.create)  → NativeJournal create-run TX
append_batch(leading=process.*) → existing transition APIs
claim_next → claim_next_ready
```
Acceptance: existing journal/process smokes still green when invoked through
the wrap; non-leading units do not insert extra commands.

**M3 — memory sink**  
Reuse `process_is_claimable` / lease fail semantics from `processes.mojo`.
Atomic multi-unit batch for reaps+claim. Smoke: claim order, lease reap,
idempotent replay empty events.

**M4 — product surface**  
`runtime_uri` (`sqlite://`, `memory://`, `jsonl://`). CLI accepts `--journal`.
No requirement to drop `--db`.

**M5 — JSONL**  
Write barrier: prepare → write line + fsync → update index. Torn-line repair
on open. Reuse `json.mojo` / EmberJson.

**M6–M8** — composition polish, broader memory parity, oracle refresh.

---

## 6. Git / oracle strategy

### Option A (recommended): rebase `mojo` onto `main` 0.2.2

```text
main (0.2.2 docs + Python journal)
   │
   └── mojo rebased: keep mojo/*, tools/*, pixi; take docs from main;
       move reference/fala refresh from main src/fala
```

Pros: single history, docs/philosophy aligned, oracle matches event-stream.  
Cons: large rebase conflict surface (`reference/` vs old `src/`).

### Option B: merge main into mojo

Same content outcome; messier history.

### Option C: dual-track (short term)

Keep developing JournalPort on `mojo` without full oracle refresh; periodically
cherry-pick docs from main. Accept temporary oracle drift.

**Oracle refresh (required for honest differential):**  
After rebase, replace `reference/fala` with a snapshot of `main`’s `src/fala`
(0.2.2). Rebuild fixture producers against journal-aware APIs
(`from_journal`, etc.). Keep five-scenario oracle at first; expand when
memory/jsonl stabilize.

---

## 7. Explicit non-goals for the first Mojo event-stream cut

1. Full 71-method memory backend parity with Python conformance in one PR.
2. Event remapping parent←child (Python also defers this; bridge stays v1).
3. Dropping SQLite as default native production path.
4. Reimplementing correlation/driver/adapters for the Journal port.
5. Python interop in native binary.

---

## 8. Risk register

| Risk | Mitigation |
| --- | --- |
| Name clash: existing `journal.mojo` vs Protocol | Use `journal_port.mojo` + keep `NativeJournal` |
| Double implementation of claim policy | Single source: `processes.mojo`; SQLite SQL must stay consistent |
| Rebase destroys smoke greenness | M2 wrap only after green baseline on rebased tip |
| Oracle still 0.2.1 behavior | M8 reference refresh before claiming 0.2.2 parity |
| JSONL multi-worker claim | Document single-process only (same as Python) |

---

## 9. Success criteria (Mojo 0.2.2-class)

- [ ] Documented JournalPort + at least Sqlite + InMemory sinks
- [ ] Production default still SQLite via wrap over `NativeJournal`
- [ ] Existing Pixi smokes green; new smokes for memory + jsonl barrier
- [ ] CLI `--journal` / `--db` alias
- [ ] `runtime_uri` for nested/bridge identity
- [ ] Docs: event-first + Unix/cybernetics language on mojo tree
- [ ] `reference/fala` regenerated from Python 0.2.2 (or explicitly versioned)

---

## 10. Suggested first PR on `mojo` after rebase

**Title:** `feat(mojo): JournalPort types and SqliteJournalPort wrap`  

**Touches:** `journal_port.mojo` (types+trait), thin `sqlite_journal_port.mojo`,
one smoke, docs pointer to this file.  

**Does not touch:** correlation, driver, process host, domain_store SQL.

That single PR proves the architecture without destabilizing the native
foundation already paid for on `mojo`.
