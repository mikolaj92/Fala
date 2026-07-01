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
runtime URI or a local runtime pool id. Pool-backed delegation currently chooses
the first matching runtime; advanced policies such as least-busy and round-robin
are future work.
