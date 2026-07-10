# Adapter Contracts

Supported effector adapters:

- `python_function`: imports and calls a Python callable.
- `subprocess`: runs a local command as an argument list.
- `manual_homeostat`: opens a durable manual homeostat and waits.
- `fala_runtime`: enqueues bridge delivery to another Fala runtime or pool.

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
can check them locally, including importable `python_function` refs, subprocess
working directories, and subprocess Python script paths.
