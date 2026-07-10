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

Documents are not core ontology. Document handling lives in
`fala.domain_packs.documents` as a domain pack that maps document-shaped inputs
to impulses, associations, reactions, and projections.
