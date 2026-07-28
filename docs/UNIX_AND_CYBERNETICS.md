# Unix Streams and Cybernetic Conduction

Fala is a local autonomous **Correlator** and cybernetic mediator implemented
with Unix-shaped process composition. Cybernetics supplies the identity and
vocabulary—Impulses, conduction, Effectors, Associations, Reactions, and
Homeostats. Unix supplies the transport and durability boundary—argv children,
streams, separate journals, and interchangeable sinks.

The canonical ontology and conduction invariants live in
[`CONCEPTUAL_MODEL.md`](CONCEPTUAL_MODEL.md). Historical terminology and its
current Mojo/TOML/JSON mapping live in
[`CYBERNETIC_MAPPING.md`](CYBERNETIC_MAPPING.md).

---

## One picture

```text
 environment / operator / parent Fala
              │
              │ typed Impulse
              ▼
┌──────────────────────────────────────────────────────────┐
│ AUTONOMOUS CORRELATOR                                    │
│ named contracts → Effectors → activations and reactions  │
│ conducts relations; records ordered commands and events  │
└──────────────────────────┬───────────────────────────────┘
                           │ JournalPort
                           ▼
                  Memory · SQLite · JSONL
                           │
                       optional Tee
```

Cybernetics decides what is true about the autonomous run. Unix decides how
those truths are carried, nested, and stored.

## The Unix half

### Separation of concerns

| Layer | Responsibility | Must not know |
| --- | --- | --- |
| **Correlator** | Mediate contracts, route impulses, host local processes, and monitor execution traces | Sink-specific schemas and remote infrastructure |
| **JournalPort** | Accept ordered command/event decision units and expose work claims | Effector business logic; sink-independent atomicity claims |
| **Sink** | Materialize history as memory rows, SQLite tables, or JSONL lines | Correlation-path topology |
| **Optional ops** | Retention, bridge delivery, reaction GC, and projection rebuild | Required happy-path composition |

The JournalPort keeps supervision independent from storage. SQLite is the
reference sink, not product identity; memory and JSONL implement the same port
for tests and Unix pipes but do not inherit SQLite's transaction guarantees.

### Streams, not shared mutable files

Composition uses separate journals (or separate SQLite files) plus explicit
bridge delivery. A child never shares its parent's journal path. A parent may
import bridge envelopes or attach export/debug metadata; semantic merging is
an explicit boundary rather than a shared database.

`JsonlJournal` appends accepted batches as JSONL with a write barrier;
`TeeJournal` fans out to several sinks. Reaction bytes live in a reaction store;
the journal carries metadata and references.

### Small tools, sharp edges

- The native Mojo CLI is the operator interface.
- Effectors are `subprocess`, `native_function`, or `manual_homeostat`.
- Subprocesses receive manifests and return JSON result manifests, never open
  database handles.
- Nested Fala is another process with another journal, not a peer mesh.

Crash recovery, idempotent commands, durable claims, and rebuildable
projections are first-class SQLite/runtime concerns. Weaker sinks document
their own persistence and atomicity limits rather than claiming parity.

## The cybernetic half

Fala's public vocabulary is Impulse-first and domain-agnostic. Domain packs
(Splot, Signals, Takt, and others) map special language onto this organ; they
do not redefine its ontology. See [`CONCEPTUAL_MODEL.md`](CONCEPTUAL_MODEL.md)
for definitions and readiness rules.

The Correlator mediates named contracts rather than imposing a success-only
workflow:

1. One durable Process represents each Effector activation in a run plan.
2. Dependents become ready when every declared upstream is terminal.
3. Terminal payloads—success or error—are conducted to the receiving Effector.
4. A failed parent does not silently cancel a child; the child decides what the
   error means.

Fala claims a working lexicon and observable runtime, not formal equivalence
with any single cybernetic theory.

## How the halves lock together

| Cybernetic concern | Unix mechanism |
| --- | --- |
| Impulse accepted | `impulse.accept` command and `impulse.accepted` event in one batch |
| Effector runs | Process claim → adapter run → complete, fail, or wait |
| Conduction advances | Correlator readiness and named contract transmission |
| Homeostat opens | Process waits; explicit external completion closes it |
| Memory of the run | Ordered command/event stream materialized by a sink |
| Nested autonomy | Child Fala has a separate journal; bridge is explicit |
| Operator observation | Native CLI traces, events, and projections |

The Correlator is therefore not a SQLite file. It is the registration organ;
durability is supplied by a JournalPort sink.

## Recursion without shared-state traps

```text
Parent Fala (journal J_p)
  │
  └─ subprocess effector → Child Fala (journal J_c, J_c ≠ J_p)
                              │
                              └─ result.json / explicit bridge envelope
```

Each correlator keeps its own memory and process ownership. Composition remains
Unix-shaped; autonomy remains cybernetic.

## Design rules

1. Name cybernetically; store Unix-style.
2. Keep sinks behind JournalPort; SQLite is an implementation.
3. Treat each sink-accepted decision unit as one ordered record; require atomic
   command/event/state transitions only from a sink that explicitly provides them.
4. Give every child its own journal.
5. Map domain packs into the core; do not map the core out into domain jargon.

## Related docs

| Doc | Focus |
| --- | --- |
| [`CONCEPTUAL_MODEL.md`](CONCEPTUAL_MODEL.md) | canonical ontology and conduction |
| [`CYBERNETIC_MAPPING.md`](CYBERNETIC_MAPPING.md) | historical → current lexicon |
| [`RUNTIME_SEMANTICS.md`](RUNTIME_SEMANTICS.md) | transaction and state invariants |
| [`PROCESS_RUNTIME.md`](PROCESS_RUNTIME.md) | claims, leases, and execution |
| [`ADAPTER_CONTRACTS.md`](ADAPTER_CONTRACTS.md) | subprocess wire boundary |
| [`FALA_HOST_AND_COMPOSITION.md`](FALA_HOST_AND_COMPOSITION.md) | host composition |
