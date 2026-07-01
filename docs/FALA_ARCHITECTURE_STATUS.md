# Fala Architecture Status

Fala is now defined as an embedded, SQLite-first runtime for observable
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

Fala does not keep command aliases or fallback loaders for the old document
workflow API.

## Phase Status

| Phase | Status | Evidence |
| --- | --- | --- |
| Product definition | DONE | README and package metadata describe Fala as embedded, SQLite-first, CLI-first, serverless by default. |
| Carrier ontology | DONE | Public exports and YAML schema are Carrier-first; old document core symbols are not exported. |
| SQLite backend | PARTIAL | Reference backend exists with commands, append-only events, carriers, processes, gates, projections, artifacts, bridge inbox/outbox, and schema tracking. Further hardening can continue around broader replay guarantees. |
| External infrastructure removal | DONE | Core dependencies no longer include web, queue, Redis, Postgres, S3, or HTTP client stacks. |
| Runtime backend boundary | PARTIAL | `RuntimeBackend` covers the current runtime operations; backend conformance should continue expanding with every new mutation. |
| Artifact store | PARTIAL | Filesystem store is default and SQLite stores metadata. GC protects blobs referenced by any run, archive export records retention metadata, archive-gc deletes expired archive bundles, and SQLite vacuum exists. |
| Step adapters | PARTIAL | `python_function`, `subprocess`, `manual_gate`, and `fala_runtime` adapters exist. Subprocess uses manifests and argument-list commands. Fala-runtime processes enqueue bridge outbox deliveries and can resolve local runtime pools. |
| Commands and idempotency | PARTIAL | Runtime service mutations submit commands with idempotency keys, and command logs are inspectable through the backend and CLI. Some low-level backend put methods remain for backend implementation and tests. |
| Event log | PARTIAL | Events are ordered, command-linked, schema-versioned, SQLite-guarded against direct update/delete, and CLI-validatable by schema version. Event payload migration transforms can still expand. |
| State machines | PARTIAL | Run/process/gate statuses exist with transition checks for current command paths, including run/process transition matrix coverage, process scheduling initial-status guards, terminal process retry/complete, wait-from-running, and gate complete/cancel/expire terminal transitions. Process cancel/timed-out command paths remain future work. |
| Multi-Fala composition | PARTIAL | Runtime refs, pools, delegation policies, bridge inbox/outbox, `fala_runtime` outbox enqueue, local pool resolution, local two-SQLite delivery, file handoff, and `manual`/`least_busy`/`round_robin` pool policies exist. Network transports remain optional future work. |
| CLI | PARTIAL | Local SQLite inspection, command/event inspection, direct create/schedule commands, runtime pool/policy mutation, package-aware doctor with adapter reference checks, wait diagnostics, trace, exports, GC, `fala_runtime` delegation, pool-backed delegation, local bridge delivery, and bridge file export/import exist. Optional network transport commands remain incomplete. |
| Package schema | DONE | v2 YAML uses `carrier_types`, `carrier_relations`, capabilities, flows, and runtime config. Old package keys are rejected. |
| Domain packs | PARTIAL | Document, Splot, and Signals packs exist as Carrier mappings. More first-party domain packs can still be added. |
| Replay/export | PARTIAL | Trace, timeline, DOT, HTML, debug bundle, run archive export, and guarded deterministic `replay-execution` exist. Broader deterministic adapter coverage can still expand. |
| Docs/examples | PARTIAL | Dedicated Fala 2 conceptual, runtime, adapter, SQLite, artifact, replay, composition, domain pack, security, migration, and version policy docs exist. Carrier runtime, pipeline, Splot, Signals, and multi-Fala examples exist. |

## Next Work

1. Add more concrete domain packs only when a real workflow needs them.
2. Add optional network bridge transports if a concrete deployment needs them.
