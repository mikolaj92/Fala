# Migrations

Fala uses explicit version boundaries. Runtime data should never rely on
implicit legacy aliases or fallback parsing.

Migration kinds:

- SQLite schema migration: tracked in `schema_migrations` with the
  `runtime_backend` id.
- Package schema migration: package YAML declares `version: "2"` and must be
  parsed through the Fala package model.
- Event payload migration: events carry `schema_version`; projections must
  tolerate known event schema versions or fail loudly.
- Reaction kind migration: reaction kinds belong in Fala package/domain pack
  definitions and should use new kind ids when semantics change.
- Domain pack migration: domain packs own their domain-specific mapping changes.
- Report/profile migration: exported bundles declare their archive/report format.

Policy:

1. Additive SQLite changes get a new runtime backend schema version.
2. Breaking package changes get a new package schema version.
3. Breaking event payload changes get a new event `schema_version`.
4. Reactions are immutable; changed reaction semantics require a new reaction
   kind or metadata schema version.
5. Domain packs may provide one-way migration helpers, but core must stay
   Impulse-first.
6. Unknown versions fail validation instead of silently falling back.

## Runtime backend schema version 6 — cybernetic lexicon (breaking)

Version 6 applies the full cybernetic rename from
[CYBERNETIC_MAPPING.md](CYBERNETIC_MAPPING.md) with **no aliases and no
compatibility layer**. Databases created before version 6 are not migrated in
place: recreate them (re-run journal ingestion or start a fresh `--db` file).
The bootstrap refuses nothing silently — old table names are simply no longer
read or written.

Renamed surfaces:

- SQLite tables: `carriers`→`impulses`, `carrier_types`→`impulse_types`,
  `carrier_relations`→`impulse_relations`, `observations`→`associations`,
  `artifacts`→`reactions`, `gates`→`homeostats`; columns
  `carrier_id`→`impulse_id`, `carrier_type`→`impulse_type`; indexes renamed
  accordingly.
- Event/command literals: `carrier.accept(ed)`→`impulse.accept(ed)`,
  `observation.record(ed)`→`association.record(ed)`,
  `artifact.record(ed)`→`reaction.record(ed)`,
  `gate.save/opened/completed/expired`→`homeostat.*`.
- Adapter kinds: `manual_gate`→`manual_homeostat`.
- Package YAML keys: `carrier_types`→`impulse_types`,
  `carrier_relations`→`impulse_relations`,
  `observation_kinds`→`association_kinds`, `artifact_kinds`→`reaction_kinds`,
  `flows`→`correlation_paths` (with `steps`→`effectors`,
  `needs`→`conduction`), capability keys `accepts_carrier_types` etc. →
  `accepts_impulse_types`, `accepts_reaction_kinds`, `emits_impulse_types`,
  `emits_reaction_kinds`, `emits_association_kinds`,
  `runtime.artifact_store`→`runtime.reaction_store`; canonical file name
  `carrier-package.yaml`→`fala-package.yaml`.
- Effector subprocess contract: env vars `FALA_STEP_MANIFEST`/
  `FALA_STEP_OUTPUT_DIR`→`FALA_EFFECTOR_MANIFEST`/`FALA_EFFECTOR_OUTPUT_DIR`.
- Reaction refs: URI scheme `fala-artifact://`→`fala-reaction://`.
- Python modules: `fala.carrier_runtime`→`fala.runtime`,
  `fala.flows`→`fala.correlation_paths`, `fala.artifacts`→`fala.reactions`.

## Behavior changes shipped alongside version 6

- Idempotency keys use the `{verb}:{entity_id}` format (e.g.
  `impulse.accept:impulse_case`).
- Claiming a process journals `process.claim` (command) and `process.claimed`
  (event) inside the claim transaction; claims are run-scoped by default and
  cross-run claiming requires an explicit `all_runs=True`.
- Worker leases are enforced: `transition_process` rejects actors that do not
  hold the claim lease.
- `waiting` processes may transition directly to `succeeded`/`failed`;
  `runs observe` (CLI) and `observe_run` (service) expose the run boundary,
  and projections carry a `stale` flag stamped from the journal watermark.
- `spawned_runs` budgets are enforced at enqueue time
  (`FalaBudgetExceeded`); `close_delegations` closes out delegated runs.
- Subprocess effectors receive a minimal environment (`PATH`, `HOME`,
  `TMPDIR`, `LANG`, `LC_ALL`, `TZ`) plus explicit `env` values; ambient
  variables require opt-in via `inherit_env` (subprocess adapters only).
  Breaking for effectors that relied on inherited environment.
- `process.completed`/`process.failed` payloads carry `attempt`,
  `input_digest`, `output_digest`/`error_digest`; `reaction.recorded` carries
  `content_hash`. `fala replay-execution --compare` reports a structural diff
  of recorded vs re-run output.
