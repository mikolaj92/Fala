# Fala Process Runtime

Fala processes are schedulable execution units attached to a run and optionally
to a carrier. They are part of the embedded Carrier runtime and persist in the
SQLite backend.

## Runtime Boundary

Fala owns:

- run state
- carrier state
- process scheduling
- claim and lease state
- retry and timeout state
- command idempotency
- event append
- observation append
- artifact metadata
- gate state
- projection rebuilds
- bridge outbox/inbox records

Adapters own execution only. They receive validated input from the runtime and
return validated output for the runtime to commit.

## Process State

Current Carrier process statuses are:

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

Carrier package steps declare adapters:

```yaml
flows:
  - id: basic
    steps:
      - id: normalize
        capability: normalize
        adapter:
          kind: python_function
          ref: examples.steps.normalize_text
```

Supported adapter kinds are:

- `python`: importable Python function.
- `subprocess`: local command as an argument list.
- `manual_gate`: explicit operator gate.
- `fala_runtime`: delegation to another Fala runtime through bridge outbox.
  `runtime_ref` may be a runtime URI or a local runtime pool id. Runtime pools
  support `manual`, `least_busy`, and `round_robin` policies.

Subprocess commands are lists, not shell strings. The runtime prepares input
manifests, captures stdout/stderr, validates output manifests, and commits
resulting events/artifacts/observations transactionally.

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

Waits and deadlocks are diagnosed from persisted process/gate state:

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
- gate waits
- projection rebuilds
- restart recovery
- command deduplication

External queues and web servers are not required for local execution.
