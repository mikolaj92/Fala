# Multi-Fala Composition

Fala composition uses references and bridge delivery, not global transactions.

Core pieces:

- `RuntimeRef`: identifies another runtime.
- `RunRef`: identifies a run in another runtime.
- `EventRef`: identifies a source event.
- `RuntimePool`: groups candidate runtimes.
- `DelegationPolicy`: stores carrier type filters and bridge budget.
- bridge outbox/inbox: durable local delivery records.

`fala_runtime` steps enqueue bridge outbox deliveries. A `runtime_ref` may be a
runtime URI or a local runtime pool id.

Runtime pool policies:

- `manual` / `first`: choose the first runtime in the pool.
- `least_busy`: choose the runtime with the lowest declared `metadata.load` or
  `metadata.pending_processes`.
- `round_robin`: rotate through runtimes and persist the cursor in pool
  metadata.

Bridge delivery modes:

- local SQLite delivery: `fala bridge deliver --target-db ...`
- file handoff: `fala bridge export --out delivery.json` and
  `fala bridge import --file delivery.json`
