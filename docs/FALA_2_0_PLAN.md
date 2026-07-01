# Fala 2.0 Implementation Plan

This document captures the corrected Fala 2.0 direction after the repository
audit. It is a planning document, not a claim that the current implementation
already satisfies every item.

## Target architecture

Fala 2.0 should be an embedded, composable runtime for observable information
flows. It should move typed information carriers through process graphs and
record durable state, events, artifacts, observations, gates, projections,
lineage, and audit data.

The runtime backend architecture is plugin-based:

- Core Fala defines the backend contract.
- The default distribution ships and supports only the SQLite backend plugin.
- SQLite is the documented, tested, zero-infra path.
- Other backend implementations may exist as external plugins, but they are not
  part of the default distribution or the recommended Fala 2.0 deployment path.

The current document workflow model should become a compatibility/domain pack on
top of the carrier model. `Document` remains useful as one domain vocabulary,
but `Carrier` is the core runtime concept.

## Non-goals for the default distribution

- Do not require Postgres, Redis, Kafka, RabbitMQ, NATS, Docker, or a web server
  to run the core runtime.
- Do not treat any non-SQLite storage backend as first-party default runtime
  infrastructure.
- Do not make the web/API surface a dependency of embedded execution.
- Do not keep document-specific concepts inside the runtime core once the
  carrier model exists.

## Current repository implications

The current implementation already has useful pieces:

- `SQLiteStateStore` with WAL, busy timeout, foreign keys, migrations, events,
  claims, outputs, projections, streams, and audit data.
- `RuntimeService` as a host-side facade.
- subprocess, HTTP, queue, and manual adapters.
- content-addressed local artifact storage.
- CLI, optional API/web surfaces, trace, lineage, reductions, dead-letter and
  stuck-work reports.

The current implementation also has mismatches with the Fala 2.0 target:

- The legacy process runtime still uses `Document` as its public vocabulary.
- `Carrier`, `CarrierType`, `CarrierRelation`, `Observation`, `RuntimeBackend`,
  `RuntimeRef`, `RunRef`, and `EventRef` exist for the new Carrier-first path,
  but they do not yet cover every process/run scheduling operation.
- The legacy storage boundary is still `StateStore`; the new `RuntimeBackend`
  contract exists beside it rather than replacing it everywhere.
- Postgres appears as first-party code and documentation today. For Fala 2.0 it
  should be removed from the default distribution or moved out as an external
  plugin experiment.
- FastAPI/web code is useful but should remain optional around the embedded core.
- Commands, idempotency, inbox/outbox, first-class gates, and observations now
  exist in the Carrier path; projection rebuild semantics and full runtime
  scheduling commands still need a clearer design.

## Phase 1: Lock current behavior with conformance tests

Goal: create a safety net before changing runtime vocabulary and backend
boundaries.

Work items:

1. Add focused tests for existing SQLite claims, leases, events, outputs,
   projections, stream chunks, checkpoints, audit events, and artifact metadata.
2. Add tests that capture current document workflow behavior so compatibility
   can be preserved while `Carrier` is introduced.
3. Add negative tests for unsafe transitions, duplicate claims, duplicate IDs,
   malformed output, invalid package references, and stuck work diagnostics.
4. Mark Postgres tests and docs as pre-2.0 compatibility debt rather than Fala
   2.0 conformance requirements.

Exit criteria:

- SQLite runtime behavior is protected by a dedicated conformance suite.
- Existing document workflows can be used as compatibility fixtures.
- Backend conformance tests do not require any external service.

## Phase 2: Define the plugin boundary and slim the core

Goal: make backend pluggability explicit without shipping multiple first-party
runtime backends.

Work items:

1. Define a `RuntimeBackend` protocol that covers the full runtime boundary:
   carriers, runs, processes, events, commands, idempotency, observations,
   gates, projections, artifacts metadata, lineage, audit, inbox, and outbox.
2. Add a backend plugin registration/loading mechanism.
3. Make SQLite the only bundled backend plugin.
4. Move or remove first-party Postgres code from the default runtime path.
5. Remove default-distribution documentation that presents Postgres as a normal
   Fala 2.0 deployment target.
6. Keep FastAPI/web integration outside the embedded core dependency path.

Exit criteria:

- Core code depends on `RuntimeBackend`, not directly on a concrete store.
- `SQLiteRuntimeBackend` is the only backend implementation shipped by default.
- Non-SQLite backends are clearly external-plugin territory.

## Phase 3: Introduce Carrier beside Document

Goal: establish the Fala 2.0 ontology without breaking current users.

Work items:

1. Extend `Carrier`, `CarrierType`, `CarrierRelation`, and carrier payload/value
   models until scheduler/service code no longer needs document core fields.
2. Map current `RuntimeDocument*` types to carrier compatibility wrappers.
3. Rename new runtime APIs around carrier semantics while keeping document API
   aliases for compatibility.
4. Move document-specific package concepts toward a document domain pack.
5. Update examples to include at least one non-document carrier flow.

Exit criteria:

- New runtime internals can operate on carriers.
- Document workflows still pass through compatibility adapters.
- New docs describe Carrier as the core concept and Document as a domain layer.

## Phase 4: Build the SQLite runtime backend plugin

Goal: promote SQLite from a state store to the canonical bundled backend plugin.

Work items:

1. Design the SQLite schema around Fala 2.0 concepts:
   - carriers and carrier relations;
   - runs and process instances;
   - command log and idempotency keys;
   - append-only events with sequence, actor, correlation, causation, and schema
     version metadata;
   - observations;
   - gate state;
   - projection state and rebuild metadata;
   - artifact metadata;
   - lineage and audit;
   - inbox/outbox for runtime composition.
2. Use SQLite-native strengths: WAL, transactions, foreign keys, online backup,
   deterministic migrations, and single-file portability.
3. Keep process claims and lease acquisition transactional.
4. Add schema migrations and backend diagnostics for the new plugin.
5. Add backend conformance tests that run entirely locally.

Exit criteria:

- `SQLiteRuntimeBackend` implements the complete backend contract.
- No external storage service is needed for any core runtime test.
- SQLite backup, migration, and diagnostics are documented.

## Phase 5: Migrate service, scheduler, and adapters to Carrier

Goal: make the runtime behavior carrier-first while preserving document
compatibility.

Work items:

1. Update scheduling and process readiness to use carriers and carrier relations.
2. Update claims, leases, retries, timeouts, and cancellation around carrier
   process instances.
3. Update subprocess/manual/queue/HTTP adapters to pass carrier context.
4. Keep document adapter aliases for compatibility.
5. Add transition validation for run, carrier, process, and gate state machines.
6. Replace acyclic-only assumptions with explicit cycle and wait-graph handling.

Exit criteria:

- Carrier flows work without document terminology.
- Document examples continue to work through the compatibility layer.
- Cycles are modeled intentionally and deadlocks can be diagnosed.

## Phase 6: Make gates, observations, and projections first-class

Goal: move important runtime concepts out of ad-hoc process metadata.

Work items:

1. Add first-class gate records with lifecycle states such as open, completed,
   cancelled, expired, and failed.
2. Add observation records for typed runtime observations, stream chunks, sensor
   values, external facts, and step-reported measurements.
3. Define projection specs, projection versions, rebuild commands, stale markers,
   and deterministic rebuild behavior.
4. Connect gates, observations, and projections to the event log and audit log.
5. Expose CLI commands for listing, inspecting, completing, and rebuilding these
   objects.

Exit criteria:

- Gates are not only manual process steps; they are queryable runtime objects.
- Observations are not only stream chunks or events; they are a first-class data
  type.
- Projections can be rebuilt and audited.

## Phase 7: Add Multi-Fala composition and runtime pools

Goal: allow multiple Fala runtimes to cooperate without requiring external
infrastructure by default.

Work items:

1. Add `RuntimeRef`, `RunRef`, and `EventRef` models.
2. Add local SQLite inbox/outbox tables for inter-runtime delivery.
3. Add bridge commands for exporting, importing, delivering, and replaying
   runtime messages.
4. Add runtime pools and delegation policies.
5. Add budgets for delegated work: attempts, wall time, carrier count, artifact
   bytes, spawned runs, and runtime hops.
6. Keep all core composition features runnable with local SQLite files.

Exit criteria:

- One runtime can delegate or exchange work with another runtime through the
  plugin boundary.
- The default path still needs only local SQLite.
- Bridge/replay behavior is testable without Redis, Kafka, RabbitMQ, NATS, or
  Postgres.

## Phase 8: Finish product docs, examples, and domain packs

Goal: make Fala 2.0 understandable and usable without reading the implementation.

Work items:

1. Write conceptual docs for Carrier, RuntimeBackend, SQLite plugin, events,
   commands, gates, observations, projections, artifacts, lineage, audit, and
   Multi-Fala.
2. Move document-specific docs and examples into a document domain pack section.
3. Add examples for:
   - basic carrier flow;
   - document compatibility flow;
   - local SQLite embedded runtime;
   - gate and observation flow;
   - Multi-Fala local bridge;
   - Splot/arbitration domain pack if it is in scope.
4. Update README to make SQLite the default shipped backend plugin and to avoid
   presenting non-SQLite backends as the recommended path.
5. Add migration notes from the current document workflow API to Fala 2.0.

Exit criteria:

- A new user can run the core runtime with only Python and SQLite.
- Docs and examples present Carrier-first terminology.
- Document workflow support is clearly compatibility/domain-pack behavior.

## Scorecard targets

| Area | Fala 2.0 target |
| --- | --- |
| Core ontology | Carrier-first |
| Documents | Domain pack / compatibility layer |
| Backend architecture | Plugin boundary |
| Bundled backend plugin | SQLite only |
| External infra | Not required for core runtime |
| Web/API | Optional integration surface |
| Events | Durable, ordered, causally linked |
| Commands | Idempotent and auditable |
| Gates | First-class runtime objects |
| Observations | First-class typed records |
| Projections | Rebuildable and versioned |
| Multi-Fala | Local-first via backend/plugin boundary |
| Tests | SQLite conformance first |

## Requirement checklist from the audit prompt

This checklist is the source of truth for whether the long Fala 2.0 audit prompt
is fully satisfied. `PARTIAL` means useful code or docs exist, but the target is
not complete enough to close the requirement.

| # | Area | Status | Next task |
| --- | --- | --- | --- |
| 1 | Product definition | PARTIAL | Make README, docs, package metadata, and examples consistently present Fala as an embedded SQLite-first information-flow runtime. |
| 2 | Core ontology: Carrier over Document | PARTIAL | Finish carrier-first API/schema names and move document names behind compatibility/domain-pack aliases. |
| 3 | Core concepts | PARTIAL | Fill gaps around process/run scheduling, refs, projection rebuilds, and clean Event/Observation/Artifact/Projection separation. |
| 4 | SQLite backend | PARTIAL | Promote SQLite backend to the complete reference backend for runs, processes, gates, commands, inbox/outbox, artifacts metadata, and projections. |
| 5 | External infrastructure | PARTIAL | Keep FastAPI/web optional and remove or isolate any remaining first-party external infra assumptions. |
| 6 | Runtime backend adapter boundary | PARTIAL | Expand `RuntimeBackend` until service/scheduler/adapters no longer rely on legacy `StateStore` for core mutations. |
| 7 | Artifact store | PARTIAL | Verify immutable filesystem artifacts, metadata-only SQLite records, GC, and large-file behavior. |
| 8 | Step adapters | PARTIAL | Harden subprocess/manual/fala-runtime contracts, manifests, audit events, and validation. |
| 9 | Commands and idempotency | PARTIAL | Route all runtime mutations through command APIs with actor, causation, correlation, and deduplication. |
| 10 | Event log | PARTIAL | Make append-only events the consistent source for projections/replay with schema/version metadata. |
| 11 | State machines | PARTIAL | Add explicit target run/process/gate statuses, transition validation, and tests. |
| 12 | Multi-Fala composition | PARTIAL | Implement delegation through runtime refs, outbox/inbox, wait graph, budgets, and no global transactions. |
| 13 | Runtime pools | PARTIAL | Add routing policies and persistence for multi-runtime pools. |
| 14 | Budgets and safety limits | PARTIAL | Enforce runtime budgets for retries, gates, artifact bytes, pending processes, spawned runs, and delegation. |
| 15 | Cycles and deadlock detection | PARTIAL | Add wait graph diagnostics and distinguish valid feedback cycles from deadlocks. |
| 16 | Web/UI | PARTIAL | Keep web optional, read-only by default, localhost-bound, and mutation-through-command only; add static export. |
| 17 | CLI | PARTIAL | Complete carrier-first inspect/mutate commands, doctor, waits diagnosis, trace, bridge, and compatibility aliases. |
| 18 | Package schema | PARTIAL | Finish carrier-first YAML, runtime config, flows, validation, and backward compatibility. |
| 19 | Domain packs | PARTIAL | Move document-specific runtime logic into `fala.domains.documents` or equivalent compatibility package. |
| 20 | Splot integration | PARTIAL | Keep Splot as a domain pack/adapter and add an end-to-end carrier/artifact/gate example. |
| 21 | Replay | PARTIAL | Define history replay, projection rebuild, deterministic execution replay, and trace export boundaries. |
| 22 | Versioning and digests | PARTIAL | Store runtime/backend/package/flow/adapter/schema versions and digests per run. |
| 23 | Migrations | PARTIAL | Cover SQLite, package, event payload, artifact kind, domain pack, and report/profile migrations. |
| 24 | Error taxonomy | MISSING | Add canonical Fala error classes and wire them into retry/fail/gate behavior. |
| 25 | Security and trust boundaries | PARTIAL | Audit subprocess/YAML/web/operator/secrets boundaries and prevent direct adapter DB mutation. |
| 26 | Secrets and redaction | PARTIAL | Resolve env secrets safely and redact them from events, reports, traces, metadata, and exports. |
| 27 | Retention and garbage collection | PARTIAL | Finish run archive, SQLite compact/vacuum, dry-run GC, artifact GC, and shared artifact safeguards. |
| 28 | Projections | PARTIAL | Make projections explicitly versioned, rebuildable, stale-detectable read models. |
| 29 | Doctor / validation | PARTIAL | Expand `fala doctor`-style checks for carrier schema, adapters, graph, SQLite status, artifact store, and bridge config. |
| 30 | Trace / debug / export | PARTIAL | Add/finish trace.json, timeline.json, graph.dot, report.html, and portable bundle export. |
| 31 | Step SDK | PARTIAL | Standardize subprocess input/output manifests and transactional runtime commit. |
| 32 | Actor model | PARTIAL | Use actor fields consistently for CLI, worker, web, adapter, bridge, and human actions. |
| 33 | Resource accounting | PARTIAL | Surface runtime, attempts, sizes, artifact count, event count, subprocess count, spawned runs, and bridge metrics. |
| 34 | Conformance tests | PARTIAL | Complete backend conformance tests for every required mutation and recovery/idempotency behavior. |
| 35 | Docs | PARTIAL | Add the target conceptual, runtime, adapter, SQLite, artifact, replay, composition, domain-pack, security, and migration docs. |
| 36 | Examples | PARTIAL | Add runnable local SQLite examples for documents, Splot/arbitration, and multi-Fala composition. |

## Open decisions

1. Should non-SQLite backend plugin interfaces live in this repository, or only
   the protocol and SQLite implementation?
2. How long should document compatibility aliases remain after Carrier becomes
   the core model?
3. Which web/API dependencies should move to extras to keep embedded core small?
4. What is the minimal Splot/arbitration domain pack for Fala 2.0?
5. Which state transitions are hard errors and which are recoverable operator
   actions?
