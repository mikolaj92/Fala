# Fala 2.0 Carrier Runtime

Fala 2.0 starts from `Carrier`, not `RuntimeDocument`.

The current Carrier-first path lives in `fala.runtime_backend`:

- `FalaRuntime` in `fala.carrier_runtime` is the embedded core facade. It uses
  `RuntimeBackendService` and does not import web, API, CLI, or HTTP-client
  modules.
- `Carrier` is the typed information unit moved by a run.
- `RuntimeCommand` is the idempotent write path.
- `RuntimeEvent` records ordered, command-linked runtime facts.
- `SQLiteRuntimeBackend` is the bundled local backend.
- `RuntimeBackendService` is the service facade for new Carrier-first writes.
- `CarrierWorkerContext` in `fala.sdk` is the worker payload/env helper for v2 adapters.
- `RuntimeRef`, `RunRef`, and `EventRef` identify other Fala runtimes, runs,
  and events without adding a non-SQLite first-party backend.
- `BridgeDelivery` records local SQLite inbox/outbox exchange. Bridge enqueue,
  import, and delivery go through idempotent `RuntimeCommand`s and emit linked
  `RuntimeEvent`s.
- `RuntimePool`, `DelegationPolicy`, and `RuntimeBudget` describe Carrier-first
  delegation targets and budgets for runtime hops, spawned runs, carriers, wall
  time, attempts, and artifact bytes.

The existing document/process runtime remains the legacy surface while the rest of
the migration lands. New Fala 2.0 runtime work should use `fala.runtime_backend`
or `fala.carrier_runtime` and should not add `RuntimeDocument`, `document_id`, or
`document_type` to the new Carrier APIs. Web/API/client exports are outer
surfaces and are loaded lazily from `fala`.

Document workflows live in `fala.domain_packs.documents`; see
`docs/DOCUMENT_DOMAIN_PACK.md` for the Document to Carrier migration mapping.
Splot arbitration workflows live in `fala.domain_packs.splot`; see
`docs/SPLOT_DOMAIN_PACK.md` for the domain/core boundary.

## Core Concepts

- Carrier: the typed unit of information moved by the runtime. It can represent
  a case, reading, event, document-domain object, or any other domain value.
- RuntimeBackend: the persistence boundary for carriers, commands, events,
  observations, gates, projections, and bridge inbox/outbox records.
- RuntimeCommand: the only write path for state-changing runtime actions.
  Commands carry an idempotency key, actor, correlation id, causation id, and
  payload.
- RuntimeEvent: ordered facts linked to commands. Events are the audit trail for
  mutations and the source for projections.
- Observation: a typed measurement or fact reported about a carrier.
- Gate: a first-class decision point such as human review, approval, expiry, or
  cancellation.
- Projection: a rebuildable read model keyed by run and projection name.
- Lineage: represented through carrier ids, event refs, bridge refs, and domain
  pack metadata rather than document-specific core fields.
- Audit: represented by command actor/correlation/causation metadata plus the
  ordered event log.
- Artifacts: represented as carrier payload or observation values in core; a
  domain pack can impose stronger artifact schemas.

## SQLite-Only Core

Fala core ships the SQLite runtime backend. Non-SQLite storage or transport
backends are external plugin work. The default Carrier-first path must run with
only Python and SQLite.

## Conformance

Reusable backend conformance checks live in
`tests/runtime_backend_conformance.py`. The shipped SQLite backend runs those
checks in `tests/test_runtime_backend_conformance.py`.

The conformance checks cover:

- carrier persistence;
- idempotent command submission;
- ordered command-linked events;
- observations, gates, and projections;
- bridge inbox/outbox persistence.

## CLI Inspection

Carrier-first SQLite state can be inspected without FastAPI or a web server:

```bash
uv run fala carriers list --db /tmp/fala-carrier.sqlite --run-id run_case
uv run fala carriers inspect --db /tmp/fala-carrier.sqlite --run-id run_case --carrier-id carrier_case
uv run fala events list --db /tmp/fala-carrier.sqlite --run-id run_case
uv run fala observations list --db /tmp/fala-carrier.sqlite --run-id run_case
uv run fala gates list --db /tmp/fala-carrier.sqlite --run-id run_case
uv run fala projections list --db /tmp/fala-carrier.sqlite --run-id run_case
```

## Local Examples

Run the local-first Carrier runtime example:

```bash
uv run python examples/carrier-runtime/local_first.py /tmp/fala-carrier.sqlite
```

The example uses one local SQLite file and exercises a non-document carrier,
observation, gate, projection, and the document domain pack.
