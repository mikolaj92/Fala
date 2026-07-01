# Events And Replay

Runtime events are ordered runtime facts. They include run, carrier, process,
actor, causation, correlation, schema version, payload, and sequence data.

Replay levels:

- History replay: `fala trace` shows what happened.
- Projection replay: `fala projections rebuild` rebuilds read models.
- Execution replay: `fala replay-execution` shows recorded process input/output.
  With `--rerun`, it reruns only processes marked `metadata.deterministic=true`
  and compares the rerun output to the recorded output without mutating SQLite.

Debug/export commands:

- `fala trace`
- `fala replay-execution`
- `fala export-html`
- `fala export-bundle`
- `fala archive-run`
