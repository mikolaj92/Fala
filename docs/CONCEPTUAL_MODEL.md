# Conceptual Model

Fala is an embedded, **event-first** runtime for observable information
correlation paths. The product identity is not “a SQLite app”; it is an
**autonomous correlator** that conducts Impulses through process graphs and
emits a durable event stream through a Journal port.

SQLite is the bundled **reference journal sink**. Memory and JSONL sinks
implement the same port for tests and Unix-style pipes.

For the full synthesis of Unix composition and cybernetic lexicon, see
[`UNIX_AND_CYBERNETICS.md`](UNIX_AND_CYBERNETICS.md).

## Core object

The core object is an **Impulse**: a typed information impulse moving through
a run-scoped process graph (`CorrelationPath` of `Effector`s).

## Core runtime records

Domain-agnostic records (the organ’s vocabulary):

| Record | Role |
| --- | --- |
| **Impulse** | Typed information payload entering the system |
| **ImpulseType** / **ImpulseRelation** | Type registry and lineage between impulses |
| **Association** | Domain reading, snapshot, score, chunk, or measurement |
| **Reaction** | Materialized output (bytes outside the journal; metadata/refs inside) |
| **Event** | Append-only runtime fact (ordered, command-linked) |
| **Command** | Idempotent write intent |
| **Process** | Schedulable unit of effector work (lease, retry, terminals) |
| **Homeostat** | Durable wait for human or external completion (defensive regulation) |
| **Projection** | Rebuildable read model from state/events |
| **Run** | Lifecycle boundary for a local execution |

Domain-specific mappings (arbitration cases, sensor samples, …) live in
`fala.domain_packs.*` outside the core ontology.

## Architecture layers

```text
AutonomousCorrelator  →  RuntimeBackendService  →  JournalBackedBackend
                                                         │
                                              Journal (memory | sqlite | jsonl)
```

- **Cybernetic layer:** Impulses, conduction, effectors, homeostats, associations.
- **Unix layer:** Journal Protocol, separate child journals, CLI streams, optional Tee.

See [`EVENT_STREAM_CORE.md`](EVENT_STREAM_CORE.md) and
[`CYBERNETIC_MAPPING.md`](CYBERNETIC_MAPPING.md).

## Relationship to Takt

Fala provides flat, observable conduction through `CorrelationPath`. Takt is a
separate package with its own local `Wave` type and regulator prototype; it is
not part of Fala's core runtime and is not required to use Fala.

An external adapter may connect a Takt-controlled plant to Fala, but such an
adapter is optional and must map the current runtime contracts explicitly.
This document does not claim that Fala implements hierarchical regulation or
formal equivalence with any cybernetic theory — only that its lexicon and
runtime are deliberately shaped for autonomous information correlation.
