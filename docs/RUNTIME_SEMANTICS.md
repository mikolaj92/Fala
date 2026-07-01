# Runtime Semantics

Runtime mutations go through `RuntimeBackendService` command paths where
available. Commands carry idempotency keys and append runtime events for durable
audit.

Run cancellation is a first-class `run.cancel` command and emits
`run.cancel_requested`.

Except for `run.create`, command submission requires the target run to already
exist.

Run-scoped backend writes also require an existing run.

Process execution:

- `ready` processes can be atomically claimed.
- claimed processes become `running` under a worker lease.
- adapters return completed output or a waiting state.
- waiting processes are persisted as `waiting`.
- failed attempts retry while attempts remain, otherwise they become `failed`.
- cancellation and timeout move non-terminal processes to `cancelled` or
  `timed_out` and clear worker leases.

Run, process, and gate status transitions are validated in the runtime backend.
Illegal terminal-state rewrites are rejected unless the same idempotent command
is replayed.

Gates move from `open` to one terminal status: `completed`, `cancelled`, or
`expired`.
