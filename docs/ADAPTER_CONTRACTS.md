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

The runtime writes the input manifest, captures stdout/stderr, redacts configured
secret values from captured streams and `output/result.json`, validates that the
result is a JSON object, and commits runtime state itself. Effectors must not mutate
SQLite directly.

`fala doctor --package` validates package adapter references where the runtime
can check them locally (known kinds, subprocess command shape, env boundary).
