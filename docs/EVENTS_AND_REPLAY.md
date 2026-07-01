# Events And Replay

Runtime events are ordered runtime facts. They include run, carrier, process,
actor, causation, correlation, schema version, payload, and sequence data.
The SQLite backend enforces append-only event storage with triggers that reject
direct event updates and deletes.

Replay levels:

- History replay: `fala trace` shows what happened.
- Projection replay: `fala projections rebuild` rebuilds read models.
- Execution replay: `fala replay-execution` shows recorded process input/output.
  With `--rerun`, it reruns only processes marked `metadata.deterministic=true`
  and compares the rerun output to the recorded output without mutating SQLite.

Debug/export commands:

- `fala events validate-schema`
- `fala trace`
- `fala replay-execution`
- `fala export-html`
- `fala export-bundle`
- `fala archive-run`
