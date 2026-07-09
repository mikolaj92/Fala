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
| SQLite backend | DONE | Reference backend exists with transactional run creation/transitions, carrier acceptance, carrier type registration, carrier relation recording, observation recording, artifact recording, process scheduling/transitions, gate creation/transitions, projection save/rebuild, and local bridge inbox/outbox mutations; append-only commands/events; schema tracking; and run-scoped writes reject unknown runs. |
| External infrastructure removal | DONE | Core dependencies no longer include web, queue, Redis, Postgres, S3, or HTTP client stacks. |
| Runtime backend boundary | DONE | `RuntimeBackend` covers the current runtime operations, including transactional `create_run`, `transition_run`, `accept_carrier`, `register_carrier_type`, `record_carrier_relation`, `record_observation`, `record_artifact`, `schedule_process`, `transition_process`, `save_gate`, `transition_gate`, `save_projection`, `rebuild_projections_with_command`, and bridge inbox/outbox mutation methods. |
| Artifact store | DONE | Filesystem store is default and SQLite stores metadata. Content-addressed blobs are verified before reuse, GC protects blobs referenced by any run, archive export records retention metadata, archive-gc deletes expired archive bundles, and SQLite vacuum exists. |
| Step adapters | DONE | `python_function`, `subprocess`, `manual_gate`, and `fala_runtime` adapters exist. Subprocess uses manifests, argument-list commands, and secret redaction for captured streams and output. Fala-runtime processes enqueue bridge outbox deliveries and can resolve local runtime pools. |
| Commands and idempotency | DONE | Flow runtime service mutations commit append-only commands, events, and state changes through backend transactions with idempotency keys, including explicit `run.cancel`; non-`run.create` commands require an existing run. Command logs are inspectable through the backend and CLI. |
| Event log | DONE | Events are ordered, command-linked, schema-versioned, SQLite-guarded against direct update/delete, and CLI-validatable by schema version. |
| State machines | DONE | Run/process/gate statuses exist with transition checks for command paths, including run/process transition matrix coverage, process scheduling initial-status guards, terminal process retry/complete/cancel/timeout, wait-from-running, and gate complete/cancel/expire terminal transitions. |
| Multi-Fala composition | DONE | Runtime refs, pools, delegation policies, bridge inbox/outbox, `fala_runtime` outbox enqueue, local pool resolution, local two-SQLite delivery, file handoff, and `manual`/`least_busy`/`round_robin` pool policies exist. Network transports remain optional non-core adapters. |
| CLI | DONE | Local SQLite inspection, command/event inspection, direct create/schedule commands, runtime pool/policy mutation, package-aware doctor with adapter reference checks, wait diagnostics, trace, exports, GC, validated run-until-idle lease/tick limits, `fala_runtime` delegation, pool-backed delegation, local bridge delivery, and bridge file export/import exist. |
| Package schema | DONE | Current YAML uses `carrier_types`, `carrier_relations`, capabilities, flows, and runtime config. Old package keys are rejected. |
| Domain packs | DONE | Document, Splot, and Signals packs exist as Carrier mappings outside the core ontology. |
| Replay/export | DONE | Trace, timeline, DOT, HTML, debug bundle, run archive export, and guarded deterministic `replay-execution` exist. Deterministic rerun is covered for `python_function` and `subprocess`; manual and Fala-runtime steps are explicit non-local replay boundaries. |
| Docs/examples | DONE | Dedicated Fala conceptual, runtime, adapter, SQLite, artifact, replay, composition, domain pack, security, migration, and version policy docs exist. Carrier runtime, pipeline, Splot, Signals, and multi-Fala examples exist and create explicit run records. |

## Optional Non-Core Work

1. Add more concrete domain packs only when a real workflow needs them.
2. Add optional network bridge transports only when a concrete deployment needs them.
