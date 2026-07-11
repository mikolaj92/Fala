# Conceptual Model

Fala is an embedded, SQLite-first runtime for observable information correlation paths.
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
