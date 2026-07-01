# Events And Replay

Runtime events are ordered runtime facts. They include run, carrier, process,
actor, causation, correlation, schema version, payload, and sequence data.

Replay levels:

- History replay: `fala trace` shows what happened.
- Projection replay: `fala projections rebuild` rebuilds read models.
- Execution replay: only safe where adapter inputs and outputs are recorded and
  deterministic. Full deterministic execution replay remains future work.

Debug/export commands:

- `fala trace`
- `fala export-html`
- `fala export-bundle`
- `fala archive-run`
