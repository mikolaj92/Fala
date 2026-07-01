# Adapter Contracts

Supported step adapters:

- `python_function`: imports and calls a Python callable.
- `subprocess`: runs a local command as an argument list.
- `manual_gate`: opens a durable manual gate and waits.
- `fala_runtime`: enqueues bridge delivery to another Fala runtime or pool.

Subprocess steps receive:

```text
input/
  manifest.json
output/
  result.json
```

The runtime writes the input manifest, captures stdout/stderr, redacts configured
secret values, validates that `output/result.json` is a JSON object, and commits
runtime state itself. Steps must not mutate SQLite directly.

`fala doctor --package` validates package adapter references where the runtime
can check them locally, including importable `python_function` refs, subprocess
working directories, and subprocess Python script paths.
