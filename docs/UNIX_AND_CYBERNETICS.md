# Unix Streams and Cybernetic Conduction

Fala sits at the intersection of two disciplined traditions:

1. **Unix process composition** — small tools, clear streams, optional sinks.
2. **Autonomous-system cybernetics** (Mazur / Kossecki lexicon) — impulses,
   conduction, effectors, homeostats, and durable associations.

Neither is decorative. The Unix half keeps the engine light and recursive.
The cybernetic half names what moves through that engine and why the run
settles, waits, or defends.

Authoritative term mapping: [`CYBERNETIC_MAPPING.md`](CYBERNETIC_MAPPING.md).  
Event-stream architecture: [`EVENT_STREAM_CORE.md`](EVENT_STREAM_CORE.md).  
Ontology: [`CONCEPTUAL_MODEL.md`](CONCEPTUAL_MODEL.md).

---

## One picture

```text
  environment / operator / parent Fala
              │
              │  Impulses (typed packets of information)
              ▼
┌─────────────────────────────────────────────────────────────┐
│  AUTONOMOUS CORRELATOR  (cybernetic organ of conduction)    │
│                                                             │
│  Contracts / Pathways ──► Effectors (Autonomous Nodes)      │
│        │ conduction             │ activations & reactions   │
│        ▼                        ▼                           │
│  Mediation (Mediate relations & monitor trace of memory)    │
│                                                             │
│  emits: ordered RuntimeCommand + RuntimeEvent stream        │
└────────────────────────────┬────────────────────────────────┘
                             │
                             │  Journal port (Unix durability boundary)
                             ▼
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
           Memory         SQLite          JSONL
           (tests)     (reference)      (pipe/file)
              │              │              │
              └──────────────┴──────────────┘
                     optional TeeJournal
```

**Cybernetics decides what is true about the world of the run.**  
**Unix decides how those truths are carried, nested, and stored.**

---

## The Unix half

### Separation of concerns

| Layer | Responsibility | Must not know |
| --- | --- | --- |
| **Core** | Mediate contracts, route impulses, run process host and monitor execution traces, enforce baseline transition boundaries | SQL schemas, file locks, Datadog, S3 |
| **Journal** | Accept atomic batches, assign sequences, claim under one lock | Effector business logic |
| **Sink** | Materialize history (maps, tables, JSONL lines) | Correlation-path topology |
| **Ops (optional)** | Retention, reaction GC, bridge outbox/inbox, heavy projection rebuild | Required for happy-path composition |

Hard-wiring SQLite into the core coupled *supervision* with *storage*. That
broke recursion (parent and child fighting one `.db`) and forced every
deployment into one składowanie model. The Journal Protocol is the Unix
fix: the engine emits facts; a sink listens.

**Essential Fala** is organ + JournalPort + driver/host + local adapters +
minimal run-to-idle/inspect CLI. Retention, maintain, reaction GC, bridge, and
projection rebuild live in `ops_*` modules — see
[`FALA_ARCHITECTURE_STATUS.md`](FALA_ARCHITECTURE_STATUS.md) and
[`JOURNALPORT_CORE_PATH.md`](JOURNALPORT_CORE_PATH.md).

### Streams, not shared mutable files

Default multi-Fala composition uses **separate journals** (or separate
SQLite files behind `SqliteJournal`) plus bridge delivery. Child never
shares the parent’s journal path. Parent can:

- import bridge envelopes (semantic merge, v1), or
- attach composition metadata with `stream.merged` helpers for export/debug.

Pipes and files stay valid sinks: `JsonlJournal` appends one line per
accepted batch with fsync-before-index barriers; `TeeJournal` fans out to
several sinks (e.g. SQLite + JSONL).

### Small tools, sharp edges

- CLI is the primary operator interface (`--journal`, `--db` alias).
- Effectors are adapters: `subprocess`, `native_function`, `manual_homeostat`.
  Subprocesses get manifests, not open DB handles. Nested Fala = another
  process + separate journal, not a peer mesh (`FALA_HOST_AND_COMPOSITION.md`).
- Reaction **bytes** live in a reaction store (filesystem by default);
  the journal holds metadata and refs only.
- Tests prefer `InMemoryJournal`; production defaults to SQLite reference sink.

### What Unix does *not* abandon

Fala is not “log to stderr and hope.” Crash recovery, idempotent commands,
atomic multi-command batches (claim reaps + auto-ready), and rebuildable
projections stay first-class — implemented **at the Journal/sink boundary**,
not by stuffing SQL into the supervisor.

---

## The cybernetic half

Fala’s public vocabulary is Impulse-first and domain-agnostic. Domain packs
(Splot, Signals, …) map special language onto this organ, not the other way
around.

| Concept | Role in the autonomous system |
| --- | --- |
| **Impulse** | Packet of information/energy entering the receptor; can represent a signal, payload, or token |
| **CorrelationPath** | Topography of named contracts defining conductivity channels between autonomous nodes |
| **Effector** | Autonomous node/organ responsible for its own decisions, reactions, and internal regulation |
| **conduction** | Realization of a named contract; Fala's act of transmitting an impulse from one node to another |
| **Process** | A concrete execution or activation attempt (impulse response) of an effector on a given impulse |
| **Association** | Micro-registration of readings / potential shifts (memory trace in the correlator) |
| **Reaction** | Materialized, permanent footprint left by an effector's execution (not automatically conducted as an impulse) |
| **Homeostat** | Defensive wait or checkpoint that maintains stability against external semantic noise |
| **Event / Command** | Ordered facts and idempotent write intents registered by the correlator |
| **Correlator** | The internal registration organ that registers states, associations, and contracts |
| **AutonomousCorrelator** | Facade of the autonomous system acting as a mediator of relations and supervisor |

Regulation hooks (e.g. `regulation` on correlation-path markers, elastic
`max_attempts` on homeostats) are entry points for quantitative damping and
variety measurements without rewriting conduction topology.

Fala does **not** claim formal equivalence with any single cybernetic theory.
It claims a **working lexicon and runtime** that make autonomous information
correlation paths observable and durable.

### Peer conduction (healthy parent / child)

Fala is a **mediator**, not a workflow tyrant:
1. **One Process per Effector (per run plan):** durable activation unit.
2. **Terminal readiness:** dependents ready when every declared upstream is
   terminal — success is not privileged over failure.
3. **Peer conduction:** terminal payloads flow under `conduction`; children
   interpret success or error themselves.
4. **No dead-upstream cancel:** failed parents do not silently kill children;
   nested Falas keep separate journals and explicit handoff.

---

## How the two halves lock together

| Cybernetic concern | Unix mechanism |
| --- | --- |
| Impulse accepted | `impulse.accept` command + `impulse.accepted` event in one batch |
| Effector runs | Process claim → adapter run → complete/fail/wait |
| Conduction advances | Mediate contract transmission; pure readiness helpers ready peers on terminal upstreams |
| Homeostat open | Process waits; external completion closes the homeostat |
| Memory of the run | Append-only event stream + materializations in a sink |
| Nested autonomy | Child Fala = separate journal; bridge or stream metadata to parent |
| Operator observation | CLI trace/events, projections, archives — sinks, not core |

The Correlator is no longer “the SQLite file.” It is the **registration organ**
whose durability is provided by a Journal sink (SQLite reference, memory,
JSONL, or tee).

---

## Recursion without shared-state traps

```text
Parent Fala  (journal J_p)
  │
  ├─ own Impulses / Processes / Events ──► J_p
  │
  └─ subprocess effector  (process host — core Fala)
        command points at child Fala CLI or any effector binary
        │
        ▼
     Child Fala  (journal J_c, J_c ≠ J_p)   # separate being
        │
        └─ events on J_c
              │
              ▼
         result.json / optional bridge file export
              │
              ▼
         Parent completes process / optional inbox import on J_p
```

Zero shared SQLite lock between parent and child. Zero peer registry.
Composition stays Unix-shaped; autonomy stays cybernetic (each correlator is
its own organ with its own memory). Multi-runtime pools are optional ops
machinery, not this picture.

---

## Design rules of thumb

1. **Name cybernetically, store Unix-style.** Impulses and homeostats in the
   API; streams and sinks on disk.
2. **Core never imports a sink driver as identity.** Prefer Journal Protocol;
   SQLite is default *implementation*, not product definition.
3. **One atomic batch = one durable decision.** Including multi-command claim
   and correlation auto-advance.
4. **Children get their own journal.** Always.
5. **Domain packs map in; core does not map out.** Splot/Signals stay outside
   the Impulse ontology.

---

## Related docs

| Doc | Focus |
| --- | --- |
| [`CONCEPTUAL_MODEL.md`](CONCEPTUAL_MODEL.md) | Core records and Takt boundary |
| [`CYBERNETIC_MAPPING.md`](CYBERNETIC_MAPPING.md) | Old → Mazur/FALA term matrix |
| [`EVENT_STREAM_CORE.md`](EVENT_STREAM_CORE.md) | Journal architecture and PR history |
| [`RUNTIME.md`](RUNTIME.md) | Runtime surfaces and concepts |
| [`MULTI_FALA_COMPOSITION.md`](MULTI_FALA_COMPOSITION.md) | Bridge, pools, delegation |
| [`PROCESS_RUNTIME.md`](PROCESS_RUNTIME.md) | Sequential driver, isolation boundary |
| [`SQLITE_BACKEND.md`](SQLITE_BACKEND.md) | Reference sink details |
| [`MOJO_EVENT_STREAM_MIGRATION.md`](MOJO_EVENT_STREAM_MIGRATION.md) | Mojo port order: **core first**, SQLite adapter second, other sinks later |
