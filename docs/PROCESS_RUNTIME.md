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

## Execution model

Default `run_until_idle` is a **claim → execute → complete** loop under one
worker lease. Its default driver is deliberately sequential: one process per
tick (`claims_per_round=1`). Logical independence in a correlation graph does
not itself promise simultaneous execution.

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
