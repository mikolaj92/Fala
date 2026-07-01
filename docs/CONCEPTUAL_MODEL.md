# Conceptual Model

Fala is an embedded, SQLite-first runtime for observable information flows.
The core object is a `Carrier`: a typed information carrier moving through a
run-scoped process graph.

Core runtime records:

- `Carrier`: typed information payload.
- `Observation`: domain reading, snapshot, score, chunk, or measurement.
- `Artifact`: materialized output stored outside SQLite with SQLite metadata.
- `Event`: append-only runtime fact.
- `Process`: scheduled unit of work over a carrier or run.
- `Gate`: durable wait for explicit human or external completion.
- `Projection`: rebuildable read model derived from runtime state/events.

Documents are not core ontology. Document handling lives in
`fala.domain_packs.documents` as a domain pack that maps document-shaped inputs
to carriers, observations, artifacts, and projections.
