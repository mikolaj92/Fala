# Cybernetic Mapping Matrix (Marian Mazur / Józef Kossecki)

This is the authoritative mapping for FALA (Functional Aggregate of Local
Association). The left column is historical terminology that no longer exists
anywhere in the codebase; the right columns are the only terms in use.

How this lexicon meets Unix process composition and the Journal port:
[`UNIX_AND_CYBERNETICS.md`](UNIX_AND_CYBERNETICS.md).

| Old (accidental)               | New (Mazur/FALA)          | Meaning |
|--------------------------------|---------------------------|---------|
| Carrier (packet)               | Impulse                   | Packet of energy/information entering the receptor |
| Flow                           | CorrelationPath           | Defined conductivity channel between states |
| Step / StepAdapter             | Effector / EffectorAdapter | Operational unit performing work or environmental reaction |
| Artifact                       | Reaction                  | Materialized, permanent footprint left by an effector |
| Observation                    | Association               | Micro-registration of potential shifts within the memory system |
| Gate                           | Homeostat                 | Defensive regulation checkpoint resisting external semantic noise |
| needs (dependency edge)        | conduction                | Conductivity line / named contract mediating interaction between effectors |
| SQLiteRuntimeBackend / Storage | Correlator                | Internal organ registering states and associations (durable via Journal sinks: SQLite reference, memory, JSONL) |
| FalaRuntime (facade)           | AutonomousCorrelator      | Execution facade of the autonomous system |
| (implicit shared DB file)      | Journal / separate sink   | Unix durability boundary; children never share the parent journal path |

All renames are **pure** (no deprecated aliases) and cover every surface:

- Python API: classes, functions, fields, module names (`fala.runtime`,
  `fala.correlation_paths`, `fala.reactions`).
- Package YAML: `impulse_types`, `impulse_relations`, `association_kinds`,
  `reaction_kinds`, `capabilities` (`accepts_impulse_types`,
  `accepts_reaction_kinds`, `emits_impulse_types`, `emits_reaction_kinds`,
  `emits_association_kinds`), `correlation_paths` (`effectors`, `conduction`),
  `runtime.reaction_store`; canonical file name `fala-package.yaml`.
- CLI: `impulses`, `impulse-types`, `impulse-relations`, `associations`,
  `reactions`, `homeostats`, `schema fala-package`, `runs observe`.
- Wire literals: `impulse.accept(ed)`, `association.record(ed)`,
  `reaction.record(ed)`, `homeostat.save/opened/completed/expired`,
  adapter kind `manual_homeostat`.
- SQLite DDL: tables `impulses`, `impulse_types`, `impulse_relations`,
  `associations`, `reactions`, `homeostats`; columns `impulse_id`,
  `impulse_type` (schema version 6, see MIGRATIONS.md).
- Contracts: `FALA_EFFECTOR_MANIFEST` / `FALA_EFFECTOR_OUTPUT_DIR` env vars,
  `fala-reaction://` URI scheme.
- Journal port: `fala.journal` (`Journal`, `InMemoryJournal`, `SqliteJournal`,
  `JsonlJournal`, `TeeJournal`, `JournalBackedBackend`); composition helpers in
  `fala.journal.stream` (`stream.merged` export metadata only — semantic multi-Fala
  merge remains bridge-based in v1).
