# Runtime Semantics

Runtime mutations go through `RuntimeBackendService` command paths where
available. Commands carry idempotency keys and append runtime events for durable
audit.

Run cancellation is a first-class `run.cancel` command and emits
`run.cancel_requested`.

Run creation is committed by the backend as one transaction that stores the run,
`run.create` command, and `run.created` event together. Direct
`submit_command(run.create)` is rejected.

Other command submission requires the target run to already exist.

Run-scoped backend writes also require an existing run.

Carrier acceptance is committed by the backend as one transaction that stores
the carrier, `carrier.accept` command, and `carrier.accepted` event together.
Carrier type registration, carrier relation recording, observation recording,
and artifact recording follow the same command/event/state transaction pattern.

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
