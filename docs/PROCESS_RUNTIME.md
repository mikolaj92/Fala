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

## Execution model

Default: `run_until_idle` is a **claim → execute → complete** loop under one
worker lease. By default it claims **one** ready process per outer step
(`claims_per_round=1`).

### Multi-claim (same journal)

Pass `claims_per_round > 1` to `drive_until_idle`, or call `drive_ready_batch`
with `max_claims`, to claim and drive several ready processes in one batch on
the **same** journal. This is still one Fala / one lease owner — not a fleet —
but it is first-class multi-claim composition for independent ready work.

### Multi-workspace (separate journals)

Unix-style parallel composition uses **multiple Fala instances**, each with
its **own journal path** (or `MemoryDriver` with a distinct `stream_id`). Nested
organs use subprocess + separate child journal (`FALA_HOST_AND_COMPOSITION.md`).
Fala is not a multi-job cluster scheduler; it is a composable organ + journal.

Implications for embedded consumers:

- **Process identity is scoped by run.** The journal primary key is
  `(run_id, process_id)`. Default correlation-path process ids are
  `{run_id}:{path_spec_id}:{effector_id}` when `correlation_path_id` is omitted.
- **Work directories follow `process.id`.** When a consumer passes
  `work_dir` into `run_until_idle`, each subprocess effector gets
  `work_dir / process.id`. That is scratch for one process, not a
  cross-run concurrent workspace manager.
- **Parallel workspaces are first-class composition.** Prefer separate journals
  (or multi-claim batches) rather than fighting one shared DB. If the host
  still starts several loops against a shared work root, isolation remains the
  consumer's responsibility for process ids and reaction store paths.
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

Fala package effectors declare adapters (TOML):

```toml
[[correlation_paths]]
id = "basic"

[[correlation_paths.effectors]]
id = "normalize"
capability = "normalize"
adapter = { kind = "native_function", ref = "example.normalize" }
```

Supported adapter kinds:

- `native_function`: registered in-process callable (Mojo registry).
- `subprocess`: local command as an argument list — **how Fala runs children**.
- `manual_homeostat`: explicit operator homeostat.

**Removed:** `python_function` (CPython), `fala_runtime` (fleet). Nest another
Fala with `subprocess` and a **separate journal**
(see [`FALA_HOST_AND_COMPOSITION.md`](FALA_HOST_AND_COMPOSITION.md)).

Subprocess commands are lists, not shell strings. The runtime prepares input
manifests, captures stdout/stderr, validates output manifests, and commits
resulting events/reactions/associations transactionally. The process host is
part of Fala product core.

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
