# Fala Process Runtime

Fala processes are schedulable execution units attached to a run and optionally
to an impulse. They are part of the embedded Impulse runtime and persist in the
SQLite backend.

## Runtime Boundary

Fala owns:

- run state
- impulse state
- process scheduling
- claim and lease state
- retry and timeout state
- command idempotency
- event append
- association append
- reaction metadata
- homeostat state
- projection rebuilds
- bridge outbox/inbox records

Adapters own execution only. They receive validated input from the runtime and
return validated output for the runtime to commit.

## Execution model (what Fala does *not* own)

`run_until_idle` is a **sequential** claim → execute → complete loop. One
driver invocation claims one ready process at a time under a worker lease,
runs its adapter, commits the result, optionally advances the correlation
path, then claims the next process. Fala is not a multi-job concurrency
scheduler and does not isolate "concurrent runs" as a first-class feature.

Implications for embedded consumers:

- **Process identity is scoped by run.** The journal primary key is
  `(run_id, process_id)`. Default correlation-path process ids are
  `{run_id}:{path_spec_id}:{effector_id}` when `correlation_path_id` is omitted.
- **Work directories follow `process.id`.** When a consumer passes
  `work_dir` into `run_until_idle`, each subprocess effector gets
  `work_dir / process.id`. That is scratch for one process, not a
  cross-run concurrent workspace manager.
- **Parallel drivers are the consumer's choice.** If the host starts several
  `run_until_idle` loops at once (threads, processes, fleet workers) against
  a shared `work_dir` or reaction store root, isolation is the consumer's
  responsibility: keep process ids unique (prefer Fala's default run-scoped
  correlation path ids; do not force a fixed `correlation_path_id` that
  omits `run_id`), and/or give each driver its own work root.
- **Leases are ownership and crash recovery**, not "orchestrate five document
  jobs in parallel for me."

## Process State

Current Impulse process statuses are:

- `pending`
- `ready`
- `running`
- `waiting`
- `retry_wait`
- `succeeded`
- `failed`
- `cancel_requested`
- `cancelled`
- `timed_out`

Runtime code must prevent arbitrary status mutation by adapters. State changes
go through backend/service operations and append runtime events.

## Adapter Kinds

Fala package effectors declare adapters:

```yaml
correlation_paths:
  - id: basic
    effectors:
      - id: normalize
        capability: normalize
        adapter:
          kind: python_function
          ref: examples.effectors.normalize_text
```

Supported adapter kinds are:

- `python_function`: importable Python function.
- `subprocess`: local command as an argument list.
- `manual_homeostat`: explicit operator homeostat.
- `fala_runtime`: delegation to another Fala runtime through bridge outbox.
  `runtime_ref` may be a runtime URI or a local runtime pool id. Runtime pools
  support `manual`, `first`, `least_busy`, and `round_robin` policies.

Subprocess commands are lists, not shell strings. The runtime prepares input
manifests, captures stdout/stderr, validates output manifests, and commits
resulting events/reactions/associations transactionally.

## Local Inspection

Processes are inspectable through the CLI:

```bash
uv run fala processes list \
  --db .fala/state.sqlite \
  --run-id run_local

uv run fala processes inspect \
  --db .fala/state.sqlite \
  --run-id run_local \
  --process-id process_123
```

Waits and deadlocks are diagnosed from persisted process/homeostat state:

```bash
uv run fala diagnose-waits \
  --db .fala/state.sqlite \
  --run-id run_local
```

## SQLite Requirements

The SQLite backend is the reference backend for process state. It must support:

- atomic claim/lease
- retry scheduling
- completion and failure commits
- homeostat waits
- projection rebuilds
- restart recovery
- command deduplication
- positive lease durations and run-until-idle tick limits

External queues and web servers are not required for local execution.
