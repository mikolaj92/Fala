# Adapter Contracts

Fala's default effector boundary is a local subprocess. All adapters execute
one claimed process; the runtime owns process state, events, reactions metadata,
and journal writes.

## Adapter kinds

- `subprocess`: local command as an argument list; the primary child boundary.
- `native_function`: registered in-process Mojo callable (embedded/tests).
- `manual_homeostat`: durable operator wait.

`python_function` and `fala_runtime` are removed product kinds. A nested Fala
uses `subprocess` and a separate journal; pool/fleet selection is not an
adapter. See [`FALA_HOST_AND_COMPOSITION.md`](FALA_HOST_AND_COMPOSITION.md).

## Subprocess wire boundary

Each attempt receives:

```text
input/manifest.json
output/result.json
```

Manifest protocol version 1 contains:

- `execution_id`: stable identity `<run_id>:<process_id>`;
- `process_id`, `attempt`, and `max_attempts`;
- `impulse_id`, `input`, `config`, and adapter metadata.

Retries preserve `execution_id` and increment `attempt`. Effectors with
external side effects should use `execution_id` as their idempotency key and
deduplicate durable results. The default `retry_policy` is `automatic`;
`none` disables automatic retry for effects that cannot be made idempotent.

The runtime gives every attempt an isolated work directory scoped by run,
process, impulse, and attempt. It writes the manifest, captures stdout/stderr,
validates `output/result.json` as a JSON object, and structurally canonicalizes
that object before committing the runtime result; the submitted JSON bytes are
not byte-preserved. Resolved secrets are redacted only from the
operator-facing stdout/stderr streams. Adapters never mutate a JournalPort,
NativeJournal, SQLite database, or other Fala journal directly.

The native `doctor --package` / `--output` filesystem checks are currently a
reserved native boundary, not an executable package-conformance command.
Package loading itself validates known adapter kinds, subprocess command shape,
and the environment boundary.

Python subprocesses may use `fala.sdk` to read
`FALA_EFFECTOR_MANIFEST`, inspect declared inputs, conduction, upstream/output
reactions, regulation, and config, then write
`FALA_EFFECTOR_OUTPUT_DIR/result.json`. These helpers do not expose manifest
adapter metadata. This is a helper for the wire contract, not a `python_function`
adapter or a CPython engine.

See [`PROCESS_RUNTIME.md`](PROCESS_RUNTIME.md) for claims and leases,
[`RUNTIME_SEMANTICS.md`](RUNTIME_SEMANTICS.md) for transaction invariants, and
[`SECURITY.md`](SECURITY.md) for the trust boundary.
