# Fala 2.0 Carrier Runtime

Fala 2.0 starts from `Carrier`, not `RuntimeDocument`.

The current Carrier-first path lives in `fala.runtime_backend`:

- `Carrier` is the typed information unit moved by a run.
- `RuntimeCommand` is the idempotent write path.
- `RuntimeEvent` records ordered, command-linked runtime facts.
- `SQLiteRuntimeBackend` is the bundled local backend.
- `RuntimeBackendService` is the service facade for new Carrier-first writes.

The existing document/process runtime remains the legacy surface while the rest of
the migration lands. New Fala 2.0 runtime work should use `fala.runtime_backend`
and should not add `RuntimeDocument`, `document_id`, or `document_type` to the new
Carrier APIs.
