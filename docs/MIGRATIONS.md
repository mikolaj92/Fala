# Migrations

Fala uses explicit version boundaries. Runtime data should never rely on
implicit legacy aliases or fallback parsing.

Migration kinds:

- SQLite schema migration: tracked in `schema_migrations` with the
  `runtime_backend` id.
- Package schema migration: packages declare `version = "2"` in current TOML
  (or equivalent JSON) and must be parsed through the Fala package model.
  Historical Fala 1 YAML is a source record for **manual conversion only**;
  the native migration loader accepts authored TOML or strict JSON, not YAML.
- Event payload migration: events carry `schema_version`; projections must
  tolerate known event schema versions or fail loudly.
- Reaction kind migration: reaction kinds belong in Fala package/domain pack
  definitions and should use new kind ids when semantics change.
- Domain pack migration: domain packs own their domain-specific mapping changes.

Policy:

1. Additive SQLite changes get a new runtime backend schema version.
2. Breaking package changes get a new package schema version.
3. Breaking event payload changes get a new event `schema_version`.
4. Reactions are immutable; changed reaction semantics require a new reaction
   kind or metadata schema version.
5. Domain packs may provide one-way migration helpers, but core must stay
   Impulse-first.
6. Unknown versions fail validation instead of silently falling back.

## Fala 1 → current cybernetic model

Fala 1's document-workflow vocabulary maps to the current cybernetic ontology
with no compatibility aliases in the core CLI or public schemas:

The document workflow was migration input, not the destination: domain-specific
document behavior moved to packs and effectors so the core could remain a
domain-agnostic Correlator. The later Carrier vocabulary was an intermediate
information-flow model; the Impulse-first rename completed the deliberate
cybernetic mapping rather than merely changing identifiers.

| Fala 1 | Current Fala |
| --- | --- |
| `Document` | `Impulse` |
| `DocumentType` | `ImpulseType` |
| `DocumentRelation` | `ImpulseRelation` |
| `DocumentRegistry` | package/domain-pack definitions |
| document workflow | correlation path |

First translate any historical YAML package to authored TOML or strict JSON;
the native migrator does not parse YAML. Then move document-specific behavior
into an external domain pack or effector and convert the package to the current
model using `impulse_types`, `impulse_relations`, `association_kinds`,
`reaction_kinds`, capabilities, `correlation_paths`, and runtime configuration.

Recommended order:

1. Convert the package to the current Impulse schema (TOML or JSON).
2. Move document-specific behavior outside the core ontology.
3. Replace document CLI usage with Impulse CLI commands.
4. Rebuild SQLite state with the current Fala runtime schema.
5. Recreate tests around impulses, associations, reactions, events, homeostats,
   and projections.

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
- Package keys (historical YAML → current TOML/JSON): `carrier_types`→`impulse_types`,
  `carrier_relations`→`impulse_relations`,
  `observation_kinds`→`association_kinds`, `artifact_kinds`→`reaction_kinds`,
  `flows`→`correlation_paths` (with `steps`→`effectors`,
  `needs`→`conduction`), capability keys `accepts_carrier_types` etc. →
  `accepts_impulse_types`, `accepts_reaction_kinds`, `emits_impulse_types`,
  `emits_reaction_kinds`, `emits_association_kinds`,
  `runtime.artifact_store`→`runtime.reaction_store`; historical canonical file name
  `carrier-package.yaml`→current `fala-package.toml` (or equivalent JSON).
- Effector subprocess contract: env vars `FALA_STEP_MANIFEST`/
  `FALA_STEP_OUTPUT_DIR`→`FALA_EFFECTOR_MANIFEST`/`FALA_EFFECTOR_OUTPUT_DIR`.
- Reaction refs: URI scheme `fala-artifact://`→`fala-reaction://`.
- Historical Python modules: `fala.carrier_runtime`→`fala.runtime`,
  `fala.flows`→`fala.correlation_paths`, `fala.artifacts`→`fala.reactions`.

## Behavior changes shipped alongside version 6

- Idempotency keys use the `{verb}:{entity_id}` format (e.g.
  `impulse.accept:impulse_case`).
- Claiming a process journals `process.claim` (command) and `process.claimed`
  (event) inside the claim transaction; claims are run-scoped by default and
  hold the claim lease.
- `waiting` processes may transition directly to `succeeded` or `failed`, and
  projections carry a `stale` flag stamped from the journal watermark.
- `spawned_runs` budgets are enforced when bridge deliveries are enqueued;
  `close_delegations` resolves parent homeostats from child run boundaries.
- Subprocess effectors receive a minimal environment (`PATH`, `HOME`,
  `TMPDIR`, `LANG`, `LC_ALL`, `TZ`) plus explicit `env` values; ambient
  variables require opt-in via `inherit_env` (subprocess adapters only).
  Breaking for effectors that relied on inherited environment.
- `process.completed`/`process.failed` payloads carry `attempt`,
  `input_digest`, `output_digest`/`error_digest`; `reaction.recorded` carries
  `content_hash`.

## Pre-release fleet ontology removal

The pre-release cleanup removed the fleet ontology from active code rather
than preserving it as compatibility surface. Current contracts are:

- active adapter kinds are `subprocess`, `native_function`, and
  `manual_homeostat`; `runtime_ref` is not an adapter field, and a
  `runtime_ref` key in a manifest or adapter JSON is an unknown field;
- `RuntimePool` and `DelegationPolicy` types are gone;
- fresh SQLite schema initialization does not create or require
  `runtime_pools`, `delegation_policies`, or `idx_delegation_policies_pool`;
- existing databases may physically retain those historical tables; they are
  ignored and are not dropped by schema bootstrap;
- generic CLI row inspection cannot inspect `runtime_pools`;
- the explicit `fala_runtime` rejection and the migration key mapping remain
  as fail-closed migration history, not as active product surface.

## Retired design documents

The documentation consolidation removed five overlapping records only after
assigning their live contracts and rationale to canonical documents:

| Retired document | Living destination | Deliberately superseded material |
| --- | --- | --- |
| `EVENT_STREAM_CORE.md` | `JOURNALPORT_CORE_PATH.md`, `RUNTIME_SEMANTICS.md`, `EVENTS_AND_REPLAY.md` | Pre-0.2.2 Python class/API proposals, QueryStore split, staged PR plan, open questions, and unshipped archive/export ideas (which remain unsupported). Its SQLite-first coupling rationale, one-authority rule, multi-command batches, idempotent replay, and atomic claims remain in the living docs. |
| `MOJO_EVENT_STREAM_MIGRATION.md` | `FALA_ARCHITECTURE_STATUS.md`, `JOURNALPORT_CORE_PATH.md`, and release history in `CHANGELOG.md` | Completed branch/work-order checklist and obsolete Python-to-Mojo port sequencing. The resulting Mojo-native event-first engine and sink contracts remain documented. |
| `MIGRATION_FROM_FALA_1.md` | This document, “Fala 1 → current cybernetic model” | Duplicate standalone checklist. Its vocabulary mapping, rationale, and five-step migration order remain above. |
| `MULTI_FALA_COMPOSITION.md` | `FALA_HOST_AND_COMPOSITION.md` | Duplicate composition guide. Separate journals, subprocess handoff, bridge envelopes, and the rejection of pools/discovery remain canonical there. |
| `RUNTIME.md` | `CONCEPTUAL_MODEL.md`, `RUNTIME_SEMANTICS.md`, `PROCESS_RUNTIME.md`, `ADAPTER_CONTRACTS.md` | Removed CPython runtime/service APIs, RuntimePool/fleet claims, `uv` examples, and unsupported replay/export/archive commands (not 0.7.16 products). Its command/event ontology, sequential driver, package runtime configuration, conduction, conformance, and inspection guidance remain in the living runtime docs. |

“Superseded” here means the old implementation plan or unsupported surface is
not part of 0.7.16. Git history is not being used as a substitute for the
motivation and current contract retained above.
