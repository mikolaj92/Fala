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
## Hierarchical regulation (kaskada)

Fala provides flat, observable conduction (CorrelationPath). Higher-level regulation — n-layer cascades of regulators with descending constraints (fala zstępująca) and ascending telemetry (fala wstępująca) — is expressed by the `takt` layer.

This follows the Polish cybernetic tradition:
- Marian Mazur, *Cybernetyczna teoria układów samodzielnych* (1966) and *Jakościowa teoria informacji* (1970): waves of communicates flow up and down; the system reduces entropy while preserving its own structure.
- Józef Kossecki: extensions to multi-level (wielopoziomowe) autonomous systems, where each level maintains its own homeostat while receiving constraints from above and reporting aggregated error signals upward.

`takt` (CascadeRegulator + TaktSequencer + ProfilHomeostatyczny) is the concrete realization of this layered control over Fala's impulses, associations, and homeostats. It does not replace Fala; it orchestrates sequences of Fala-mediated interactions across a tree of StateNodes.
