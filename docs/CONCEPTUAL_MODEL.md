# Conceptual Model

Fala is an embedded, event-first runtime for observable information correlation paths
(SQLite is the reference journal sink).
The core object is an `Impulse`: a typed information impulse moving through a
run-scoped process graph.

Core runtime records:

- `Impulse`: typed information payload.
- `Association`: domain reading, snapshot, score, chunk, or measurement.
- `Reaction`: materialized output stored outside SQLite with SQLite metadata.
- `Event`: append-only runtime fact.
- `Process`: scheduled unit of work over an impulse or run.
- `Homeostat`: durable wait for explicit human or external completion.
- `Projection`: rebuildable read model derived from runtime state/events.
Core runtime records are domain-agnostic:

- `Impulse`: typed information payload.
- `Association`: domain reading, snapshot, score, chunk, or measurement.
- `Reaction`: materialized output stored outside SQLite with SQLite metadata.
- `Event`: append-only runtime fact.
- `Process`: scheduled unit of work over an impulse or run.
- `Homeostat`: durable wait for explicit human or external completion.
- `Projection`: rebuildable read model derived from runtime state/events.

Domain-specific mappings (e.g. arbitration cases, sensor samples) live in `fala.domain_packs.*` outside the core ontology.
## Relationship to Takt

Fala provides flat, observable conduction through `CorrelationPath`. Takt is a
separate package with its own local `Wave` type and regulator prototype; it is
not part of Fala's core runtime and is not required to use Fala.

An external adapter may connect a Takt-controlled plant to Fala, but such an
adapter is optional and must map the current runtime contracts explicitly.
This document does not claim that Fala implements hierarchical regulation or
formal equivalence with any cybernetic theory.
