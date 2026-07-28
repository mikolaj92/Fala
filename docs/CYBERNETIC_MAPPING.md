# Cybernetic Mapping Matrix (Marian Mazur / Józef Kossecki)

This is Fala's authoritative historical-to-current lexicon. Cybernetic
identity is primary: Fala is a local autonomous **Correlator** that mediates
relations between effectors. JournalPort, sinks, and the process host are the
implementation surfaces that make its memory and reactions durable.

This migration was conceptual, not a cosmetic rename. The document-workflow
and generic Carrier vocabularies were too weak to express the intended
Mazur/Kossecki model of autonomous effectors, mediation, memory, and regulation.
The clean cutover deliberately left no compatibility aliases so the old
workflow ontology could not remain a second public model.

The Unix/cybernetic synthesis is described in
[`UNIX_AND_CYBERNETICS.md`](UNIX_AND_CYBERNETICS.md).

| Historical / accidental term | Current Fala term | Meaning |
| --- | --- | --- |
| Carrier (packet) | **Impulse** | Typed packet of energy/information entering a receptor |
| Flow | **CorrelationPath** | Topography of contracts defining conductivity between states |
| Step / StepAdapter | **Effector / EffectorAdapter** | Autonomous operational unit or environmental reaction |
| Artifact | **Reaction** | Materialized, durable footprint left by an effector |
| Observation | **Association** | Registration of a reading or potential shift in memory |
| Gate | **Homeostat** | Defensive regulation checkpoint resisting semantic noise |
| `needs` dependency edge | **conduction** | Named contract mediating interaction between effectors |
| SQLite runtime/backend | **Correlator + JournalPort sink** | Registration organ; SQLite is the reference sink, with memory and JSONL alternatives |
| Fala runtime facade | **AutonomousCorrelator** | Facade of the local autonomous system |
| Shared database file | **Separate JournalPort sink** | Unix durability boundary; child organs do not share a parent journal path |

## Current surfaces

The vocabulary is implemented in the Mojo product tree (`mojo/fala/`). Packages
are TOML or canonical JSON; JSON is also the subprocess wire format.

- **Mojo modules:** `domain.mojo` records (`Impulse`, `Association`,
  `Reaction`, `Homeostat`, `Projection`); `correlation.mojo` paths and
  effectors; `journal_port.mojo` batches; `memory_journal.mojo`,
  `sqlite_journal_port.mojo`, `jsonl_journal.mojo`, and `tee_journal.mojo`
  sinks; `native_process_host.mojo` for local children.
- **Package surfaces:** `load_package_toml`, `load_fala_package_toml`,
  `load_package_json`, and `load_fala_package_json`. Canonical package files
  use `.toml` or `.json`, never YAML.
- **Adapters:** `native_function`, `subprocess`, and `manual_homeostat`.
  Subprocesses receive validated JSON manifests and return JSON result
  manifests; `FALA_EFFECTOR_MANIFEST` and `FALA_EFFECTOR_OUTPUT_DIR` define
  the boundary.
- **JournalPort:** `JournalPort`, `InMemoryJournal`, `SqliteJournalPort`,
  `JsonlJournal`, and `TeeJournal` provide the durability contract. A sink
  stores ordered command/event batches; it does not define the ontology.
- **CLI:** native Mojo CLI commands inspect runs, impulses, processes,
  associations, reactions, homeostats, events, projections, and bridge
  records. CLI output is JSON; package input is TOML/JSON.

Historical names are not compatibility aliases. Current command/event pairs
include `impulse.accept`/`impulse.accepted`,
`association.record`/`association.recorded`, and
`reaction.record`/`reaction.recorded`. Homeostat commands are
`homeostat.save`, `homeostat.open`, `homeostat.complete`,
`homeostat.cancel`, and `homeostat.expire`; terminal process events include
`process.completed`, `process.cancelled`, and `process.timed_out`. The adapter
kind is `manual_homeostat`. SQLite schema tables include `impulses`,
`impulse_relations`, `associations`, `reactions`, and `homeostats`; schema
migrations are tracked in [`MIGRATIONS.md`](MIGRATIONS.md).
