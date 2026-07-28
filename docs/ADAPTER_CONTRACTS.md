# Adapter Contracts

Supported effector adapters (exclusive Mojo product):

- `subprocess`: local command as an argument list — **primary child boundary**.
- `native_function`: in-process registered callable (tests / embedded).
- `manual_homeostat`: opens a durable manual homeostat and waits.

**Removed:**

- `python_function` — CPython importable callables (not part of the product).
- `fala_runtime` (fleet/pool enqueue) — use `subprocess` + separate journal.
  See [`FALA_HOST_AND_COMPOSITION.md`](FALA_HOST_AND_COMPOSITION.md).

Subprocess effectors receive:

```text
input/
  manifest.json
output/
  result.json
```

`input/manifest.json` protocol version 1 contains:

- `execution_id`: stable logical identity scoped as `<run_id>:<process_id>`;
- `process_id`: process identity within the run;
- `attempt` and `max_attempts`: the current physical try and its durable limit;
- `impulse_id`, `input`, `config`, and adapter metadata.

Retries keep `execution_id` stable and increment `attempt`. Effectors that perform
external side effects should deduplicate durable results by `execution_id` and use
it as an idempotency key where the external system supports one.

Package effectors may set `retry_policy` to `automatic` (the compatibility
default) or `none`. `none` prevents Fala from automatically retrying adapter
failures and timeouts even if the process has attempts remaining. Use it for an
effect that cannot be made idempotent or durably deduplicated.

The default subprocess work directory is scoped by run, process, impulse, and
attempt. Attempts therefore cannot overwrite each other's manifest or result;
cross-attempt deduplication belongs to the effector and uses `execution_id`.

The runtime writes the input manifest, captures stdout/stderr, redacts configured
secret values from those operator-facing streams, validates that
`output/result.json` is a JSON object without mutating its structured content, and
commits runtime state itself. Effectors must not mutate SQLite directly.

`fala doctor --package` validates package adapter references where the runtime
can check them locally (known kinds, subprocess command shape, env boundary).
