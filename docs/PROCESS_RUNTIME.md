# Fala Process Runtime

Processes are schedulable execution units attached to a run and optionally to
an Impulse. Their persistence boundary depends on the configured sink. In the
current SQLite core, `NativeJournal` and `NativeDomainStore` own direct
transactional helpers; SQLite is the reference sink, not the process identity.

## Runtime boundary

Fala owns:

- run and impulse state;
- process scheduling, claims, and worker leases;
- retry and timeout state;
- command idempotency and event append;
- association and reaction metadata;
- homeostat state and projection rebuilds;
- bridge outbox/inbox records.

Adapters own execution only. They receive validated JSON input at the adapter
boundary and return validated output for the Correlator to commit.

## Explicit compensation

An effector may declare `compensation = { path_id, capability }`, where the
capability is deliberately distinct from the original effect capability. This
is a named child-path contract, not implicit rollback: only an authored graph
edge invokes it after a confirmed effect receipt exists. The child input carries
the exact authoritative identity/evidence receipt and a stable idempotency key.
It observes before acting and records one of `compensated`, `already_absent`,
`compensation_failed`, or `not_compensable`. Retry after a crash observes and
confirms instead of duplicating reversal. Ordinary failure never schedules
compensation, and original history is immutable. These are saga-like external
effects, not ACID transactions across systems.

## Execution model

Default `run_until_idle` is a **claim → execute → complete** loop under one
worker lease. Its default driver is deliberately sequential: one process per
tick (`claims_per_round=1`). Logical independence in a correlation graph does
not itself promise simultaneous execution.

## Conditional conduction

An effector may declare one deterministic condition over a direct upstream
output:

```toml
when = { upstream = "review", path = "decision.verdict", equals = "approve" }
```

Fala waits for every declared conduction dependency. It then reads the named
object path from the schema-projected output of the successful upstream.
A match makes the effector ready. A non-match records the effector as
`skipped` with `condition_not_met`, without executing its adapter. Missing
keys, malformed declarations, non-scalar values, and a non-successful condition
source fail closed. The comparison has no domain semantics: Fala compares the
declared JSON scalar and leaves the status vocabulary to the product graph.

`when.upstream` must also appear in the effector's direct `conduction` list.
This keeps branch evidence local, durable, and visible in the correlation path.

### Serial multi-claim loop (same journal)
Pass `claims_per_round > 1` to `drive_until_idle`, or call
`drive_ready_batch` with `max_claims`, to permit several claim/execute/complete
ticks in one driver round. The current driver still executes them sequentially
under one lease owner; this is neither an atomic claim batch nor concurrent
execution, and exact persistence behavior depends on the selected sink.

This remains one Fala, not a fleet.

Durable cancellation requests and terminal transitions do not interrupt an
already-blocked adapter call. The process host enforces its own timeout and has
a cancellation ABI, but the current driver has no live child-handle polling
path that connects a later journal cancellation request to that ABI.

Low-level journal/process retry primitives are policy-neutral. The native
driver enforces `retry_policy` for adapter failure, timeout, and expired-lease
maintenance; callers invoking low-level retry APIs directly own that policy.

External effects under automatic retry are delivered at least once: a timeout
or crash can leave an external effect completed before the runtime result is
committed, and a later attempt may run again. `execution_id` is the stable
idempotency key across attempts; `attempt` identifies only the physical try and
must not be used as that key. Effectors must durably deduplicate before
performing an external effect. Set `retry_policy = "none"` when that guarantee
cannot be made.

Native process-host library discovery is explicit: use the absolute path in
`FALA_PROCESS_HOST_LIBRARY` when set; otherwise use only the packaged library
relative to the executable. There is no cwd or source-tree fallback.

### Multi-workspace (separate journals)

Unix-style parallel composition uses multiple Fala instances, each with its own
journal path (or a memory driver with a distinct `stream_id`). Nested organs
use a subprocess and a separate child journal; see
[`FALA_HOST_AND_COMPOSITION.md`](FALA_HOST_AND_COMPOSITION.md).

Fala is not a cluster scheduler. Process identity is scoped by run: the journal
key is `(run_id, process_id)`, and default correlation-path ids are
`{run_id}:{path_spec_id}:{effector_id}` when `correlation_path_id` is omitted.
The native driver leaves `EffectorRequest.work_dir` empty. A direct adapter
caller may provide it explicitly; otherwise the subprocess adapter chooses
`FALA_EFFECTOR_ROOT` or the host current directory and creates a hashed,
per-attempt `.fala-effector-*` directory from run, process, impulse, and attempt.
Consumers remain responsible for distinct journal and reaction-store paths.

Leases provide ownership and crash recovery; they do not orchestrate arbitrary
parallel document jobs.

## Process state

Current process statuses are:

`pending`, `ready`, `running`, `waiting`, `retry_wait`, `succeeded`, `failed`,
`cancel_requested`, `cancelled`, and `timed_out`.

Adapters cannot mutate these statuses directly. State changes go through
Correlator operations and append runtime events. See
[`RUNTIME_SEMANTICS.md`](RUNTIME_SEMANTICS.md) for transaction and transition
invariants.

## Adapter kinds

Fala package effectors declare adapters in TOML (canonical JSON is equivalent):

```toml
[[correlation_paths]]
id = "basic"

[[correlation_paths.effectors]]
id = "normalize"
capability = "normalize"
adapter = { kind = "native_function", ref = "example.normalize" }
```

Supported adapter kinds:

- `native_function`: registered in-process Mojo callable.
- `subprocess`: local command as an argument list—the process-host boundary.
- `manual_homeostat`: explicit operator homeostat.

Subprocess commands are argument lists, not shell strings. The host prepares
JSON input manifests, captures stdout/stderr, validates result manifests, and
commits resulting events, reactions, and associations through the applicable
native SQLite transaction helpers when using the SQLite core.

### Bounded authoring expansion

A package may define `[[path_templates]]` once and materialize a finite list of
instances in a correlation path. Expansion happens while loading and validating
the package, before a run is created. The runtime therefore receives only an
ordinary `CorrelationPath`: expanded IDs, dependencies, and order are visible in
`serialize_package_json`, and that canonical topology produces the path digest.

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

`max_items` is mandatory. `items` may be empty or contain at most that many
objects. Parameter types are `string`, `integer`, `number`, or `boolean`.
Missing, unknown, or mistyped parameters fail closed. Expanded effector IDs
must still be globally unique inside the path. With `serial = true`, the first
effector of each instance with no authored dependencies explicitly conducts
from the previous instance's final effector; this is graph materialization, not
a scheduler or adapter loop. Existing schema-v2 packages using `effectors`
continue unchanged.

### Typed path contracts

A correlation path may declare an `input_schema` and a closed set of
`terminals`. Input is validated before run creation. After finalization, exactly
one terminal must match its source effector's terminal process status and
optional value condition; zero or multiple matches fail closed. The selected
values are validated against that terminal's independent `output_schema`.

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
Per-effector results remain available for inspection. Terminal selection and
schema validation are replay-stable because both the expanded path contract and
result are canonical and the durable run pins `correlation_path_digest`.
Paths without `terminals` retain their existing behavior and return a null path
result. Names such as `delivered`, `waiting`, `repairable`, and `failed` are
consumer-domain examples only; Fala assigns them no built-in meaning.

Current package schema version 2 declares the durable runtime boundary
explicitly:

```toml
[runtime.backend]
kind = "sqlite"
path = ".fala/state.sqlite"

[runtime.reaction_store]
kind = "filesystem"
root = ".fala/reactions"
```

This configuration selects implementations; neither SQLite nor the filesystem
reaction store defines Fala's ontology.

## Local inspection

Use the native Mojo CLI to inspect persisted processes and waits. Commands emit
JSON and accept the Journal/sink path explicitly; see the CLI help for the
current command names and options. The durable records include process state,
homeostat state, lease ownership, and wait diagnostics.

External queues and web servers are not required for local execution.
