# Runtime Semantics

Runtime mutations go through `RuntimeBackendService` command paths where
available. Commands carry idempotency keys and append runtime events for durable
audit.

Process execution:

- `ready` processes can be atomically claimed.
- claimed processes become `running` under a worker lease.
- adapters return completed output or a waiting state.
- waiting processes are persisted as `waiting`.
- failed attempts retry while attempts remain, otherwise they become `failed`.

Run, process, and gate status transitions are validated in the runtime backend.
Illegal terminal-state rewrites are rejected unless the same idempotent command
is replayed.

Gates move from `open` to one terminal status: `completed`, `cancelled`, or
`expired`.
