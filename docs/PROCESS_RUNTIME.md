# Fala Process Runtime

Processes are schedulable execution units attached to a run and optionally to
an Impulse. Persistence depends on the sink. In SQLite, `NativeJournal` and
`NativeDomainStore` own the transactions.

Fala owns run and impulse state; process scheduling, claims, and leases;
retry and timeout; command idempotency and event append; association and
reaction metadata; homeostat state; projection rebuilds; bridge
outbox/inbox. Adapters own execution only: validated JSON in, validated
JSON out.

## Execution model

Default `run_until_idle` is the parent observation loop: **claim → ask →
record** under one worker lease. The child is a separate autonom. The
default driver is sequential: one process per tick (`claims_per_round=1`).
Graph independence does not itself promise simultaneous execution.

`fala.host_run_package` records request creation time and opts the native
runtime into a fresh UTC wall-clock for process claims, terminals, run
finalization, and correlation-created/skipped transitions. Journal
timestamps are second-resolution UTC (`YYYY-MM-DDTHH:MM:SSZ`). The low-level
`native.host_run_package(JSON)` binding stays deterministic unless
`realtime_timestamps` is `true`.

Pass `claims_per_round > 1` to `drive_until_idle`, or call
`drive_ready_batch` with `max_claims`, for several sequential
claim/execute/complete ticks in one driver round. This is not an atomic
claim batch and not concurrent execution.

Durable subprocess cancellation polls the journal while retaining the live
process-host handle. `cancel_requested` records the operator request, then
`drive_once` sends SIGTERM to the private process group and may escalate to
SIGKILL. `native_function` cannot be preempted. `manual_homeostat` is
cancelled in the journal. After driver death, recovery reclaims the lease
only; OS process ownership is not reconstructed from a stale PID.

Automatic retry is at-least-once for external effects. `execution_id` is the
stable idempotency key; `attempt` is only the physical try. Effectors must
durably deduplicate before an external effect, or set
`retry_policy = "none"`. The native driver enforces `retry_policy` for
adapter failure, timeout, and expired-lease maintenance.

Native process-host discovery: `FALA_PROCESS_HOST_LIBRARY` if set, otherwise
the packaged library relative to the executable. No cwd or source-tree
fallback.

Process identity is `(run_id, process_id)`. Default correlation-path ids are
`{run_id}:{path_spec_id}:{effector_id}` when `correlation_path_id` is
omitted. The native driver leaves `EffectorRequest.work_dir` empty; the
subprocess adapter then chooses `FALA_EFFECTOR_ROOT` or the host cwd and
creates a hashed `.fala-effector-*` directory. Parallel composition is
separate Fala instances with separate journals — see
[`FALA_HOST_AND_COMPOSITION.md`](FALA_HOST_AND_COMPOSITION.md).

## Conditional conduction

An unconditional `conduction` edge is sequencing and terminal-data delivery,
not a success gate. Dependents become ready when every declared upstream is
terminal. Success output or error object is placed under
`input.conduction.<upstream-id>`. A failed or timed-out upstream still makes
the run fail at finalization.

When a successful upstream value is required, author `when` over that value.
The conditional adapter runs only when the source succeeded and the declared
scalar matches; otherwise the process is `skipped`.

```toml
when = { upstream = "review", path = "decision.verdict", equals = "approve" }
```

`when.upstream` must also appear in the effector's direct `conduction` list.
Missing keys, malformed declarations, non-scalar values, and a
non-successful condition source fail closed. Fala compares the JSON scalar;
it assigns no domain meaning.

## Explicit compensation

An effector may declare `compensation = { path_id, capability }` with a
capability distinct from the original effect. Only an authored graph edge
invokes it after a confirmed effect receipt exists. The child observes
before acting and records `compensated`, `already_absent`,
`compensation_failed`, or `not_compensable`. Ordinary failure never
schedules compensation. Original history is immutable.

## Process state

`pending`, `ready`, `running`, `waiting`, `retry_wait`, `succeeded`,
`failed`, `skipped`, `cancel_requested`, `cancelled`, `timed_out`.

Adapters cannot mutate these statuses. A nonmatching `when` records
`skipped` without invoking the adapter. See
[`RUNTIME_SEMANTICS.md`](RUNTIME_SEMANTICS.md).

## Adapter kinds

```toml
[[correlation_paths]]
id = "basic"

[[correlation_paths.effectors]]
id = "normalize"
capability = "normalize"
adapter = { kind = "native_function", ref = "example.normalize" }
```

- `native_function`: registered in-process Mojo callable.
- `subprocess`: argv list; process-host boundary.
- `manual_homeostat`: explicit operator homeostat.
- `child_path`: nested package path compiled by the Python host; see
  [`ADAPTER_CONTRACTS.md`](ADAPTER_CONTRACTS.md).

## Bounded authoring expansion

A package may define `[[path_templates]]` once and materialize a finite list
of instances. Expansion happens while loading, before run creation. The
runtime receives an ordinary `CorrelationPath`. Canonical inspection and
path digests see the full graph.

```toml
[[path_templates]]
id = "slot"
parameters = { index = "integer", repo = "string" }

[[path_templates.effectors]]
id = "prepare_${index}"
config = { repo = "${repo}" }
adapter = { kind = "manual_homeostat" }

[[path_templates.effectors]]
id = "finish_${index}"
conduction = ["prepare_${index}"]
adapter = { kind = "manual_homeostat" }

[[correlation_paths]]
id = "slots"

[correlation_paths.expansion]
template = "slot"
max_items = 25
serial = true
items = [
  { index = 0, repo = "alpha" },
  { index = 1, repo = "beta" },
]
```

`max_items` is mandatory. Parameter types are `string`, `integer`, `number`,
or `boolean`. With `serial = true`, the first effector of each instance with
no authored dependencies conducts from the previous instance's final
effector. This is graph materialization, not a host loop.

## Typed path contracts

A correlation path may declare an `input_schema` and a closed set of
`terminals`. Input is validated before run creation. After finalization,
exactly one terminal must match; zero or multiple matches fail closed.

```toml
[correlation_paths.input_schema]
type = "object"
required = ["ticket"]
properties = { ticket = { type = "integer" } }

[[correlation_paths.terminals]]
id = "delivered"
source_effector = "merge"
status = "succeeded"
when = { path = "state", equals = "delivered" }
output_schema = { type = "object", required = ["state"] }
```

The host returns `path_result = { terminal, values, evidence, path_digest }`.
Paths without `terminals` return a null path result. Names such as
`delivered` are consumer-domain examples only.

Package schema version 2 selects implementations; it does not define the
ontology:

```toml
[runtime.backend]
kind = "sqlite"
path = ".fala/state.sqlite"

[runtime.reaction_store]
kind = "filesystem"
root = ".fala/reactions"
```

Native CLI inspects persisted processes as JSON. External queues and web
servers are not required.
