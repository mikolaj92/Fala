# Events And Replay

Runtime events are ordered runtime facts. They include run, impulse, process,
actor, causation, correlation, schema version, payload, and sequence data.
The SQLite backend enforces append-only event storage with triggers that reject
direct event updates and deletes. This append-only and transaction behavior is
the current SQLite core guarantee; generic JournalPort sinks need not provide
the same persistence or multi-unit atomicity.

Replay levels:

- History replay: `fala trace` shows what happened.
- Projection replay: `fala projections rebuild` rebuilds read models, with
  `run_summary` as the rich built-in projection; arbitrary projection names are
  generic read-model names.

The SQLite event stream preserves normative per-run ordering, sequence numbers,
linked commands, append-only history, and replayable projections. Replay is a
history and projection concern; this document does not promise re-execution,
report export, bundle export, or run archiving products, nor does it claim
uniform replay or atomicity across memory, JSONL, Tee, and SQLite sinks.

Implemented inspection/validation commands (native CLI):

- `fala events validate-schema --db <path>`
- `fala trace --db <path> --run-id <id>`
- `fala projections rebuild --db <path> --run-id <id> --now <timestamp>`
