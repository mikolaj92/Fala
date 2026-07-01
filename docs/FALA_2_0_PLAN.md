# Fala 2.0 Architecture Status

Fala 2.0 is now defined as an embedded, SQLite-first runtime for observable
information flows.

The core ontology is Carrier-first:

- `Carrier`
- `CarrierType`
- `CarrierRelation`
- `Observation`
- `Artifact`
- `Event`
- `Process`
- `Run`
- `Gate`
- `Projection`
- `RuntimeBackend`
- `ArtifactStore`
- `StepAdapter`
- `RuntimeRef`
- `RunRef`
- `ArtifactRef`
- `EventRef`

The default distribution is intentionally small:

- SQLite is the bundled runtime backend.
- The filesystem is the bundled artifact store.
- The CLI is the primary operator interface.
- Web/API servers, external queues, Redis, Postgres, Kafka, RabbitMQ, NATS,
  Docker, and object-storage SDKs are not core runtime requirements.

## Current Implementation

The migrated source tree keeps the core runtime in:

- `src/fala/models.py`
- `src/fala/runtime_backend.py`
- `src/fala/carrier_runtime.py`
- `src/fala/artifacts.py`
- `src/fala/adapters.py`
- `src/fala/sdk/__init__.py`
- `src/fala/yaml_loader.py`
- `src/fala/cli.py`
- `src/fala/errors.py`

Domain-specific vocabulary lives outside core under `src/fala/domain_packs`.
The document pack is a domain pack, not the runtime ontology.

## Removed From Core

The old document workflow runtime, web/API layer, queue bridge, worker CLI,
state-store layer, project scaffolding, package registry, and deployment helpers
were removed from the core source tree.

Fala 2.0 does not keep command aliases or fallback loaders for the old document
workflow API.

## Phase Status

| Phase | Status | Evidence |
| --- | --- | --- |
| Product definition | DONE | README and package metadata describe Fala as embedded, SQLite-first, CLI-first, serverless by default. |
| Carrier ontology | DONE | Public exports and YAML schema are Carrier-first; old document core symbols are not exported. |
| SQLite backend | PARTIAL | Reference backend exists with commands, events, carriers, processes, gates, projections, artifacts, bridge inbox/outbox, and schema tracking. Further hardening is still needed around migrations and replay guarantees. |
| External infrastructure removal | DONE | Core dependencies no longer include web, queue, Redis, Postgres, S3, or HTTP client stacks. |
| Runtime backend boundary | PARTIAL | `RuntimeBackend` covers the current runtime operations; backend conformance should continue expanding with every new mutation. |
| Artifact store | PARTIAL | Filesystem store is default and SQLite stores metadata. GC protects blobs referenced by any run, archive export records retention metadata, and SQLite vacuum exists. Archive expiry enforcement can still expand. |
| Step adapters | PARTIAL | `python_function`, `subprocess`, `manual_gate`, and `fala_runtime` adapters exist. Subprocess uses manifests and argument-list commands. Fala-runtime processes enqueue bridge outbox deliveries and can resolve local runtime pools. |
| Commands and idempotency | PARTIAL | Runtime service mutations submit commands with idempotency keys. Some low-level backend put methods remain for backend implementation and tests. |
| Event log | PARTIAL | Events are ordered and command-linked. Event schema/version migration needs continued hardening. |
| State machines | PARTIAL | Run/process/gate statuses exist with transition checks for key paths. More illegal-transition tests are needed. |
| Multi-Fala composition | PARTIAL | Runtime refs, pools, delegation policies, bridge inbox/outbox, `fala_runtime` outbox enqueue, local pool resolution, local two-SQLite delivery, and `manual`/`least_busy`/`round_robin` pool policies exist. Cross-host delivery remains future work. |
| CLI | PARTIAL | Local SQLite inspection, direct create/schedule commands, runtime pool/policy mutation, package-aware doctor, wait diagnostics, trace, exports, GC, `fala_runtime` delegation, pool-backed delegation, and bridge delivery exist. Cross-host bridge commands remain incomplete. |
| Package schema | DONE | v2 YAML uses `carrier_types`, `carrier_relations`, capabilities, flows, and runtime config. Old package keys are rejected. |
| Domain packs | PARTIAL | Document and Splot packs exist as Carrier mappings. More examples and package manifests are needed. |
| Replay/export | PARTIAL | Trace, timeline, DOT, HTML, debug bundle, run archive export, and guarded deterministic `replay-execution` exist. Broader deterministic adapter coverage can still expand. |
| Docs/examples | PARTIAL | Dedicated Fala 2 conceptual, runtime, adapter, SQLite, artifact, replay, composition, domain pack, security, migration, and version policy docs exist. More runnable examples can still be added. |

## Next Work

1. Add archive expiry enforcement for retained archive bundles.
2. Add cross-host bridge transport adapters beyond local SQLite delivery.
