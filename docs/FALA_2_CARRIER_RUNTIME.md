# Fala 2.0 Carrier Runtime

Fala 2.0 starts from `Carrier`, not `RuntimeDocument`.

The current Carrier-first path lives in `fala.runtime_backend`:

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
and should not add `RuntimeDocument`, `document_id`, or `document_type` to the new
Carrier APIs.
