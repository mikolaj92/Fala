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

Retries preserve `execution_id` and increment `attempt`. Automatic retry is
at-least-once delivery for external effects: a timeout or crash may leave an
external effect completed even when the runtime result is not committed, and a
later attempt may execute again. `attempt` identifies only the physical try; it
is not an idempotency key. Effectors must durably deduplicate by stable
`execution_id` before performing the external effect. If that guarantee cannot
be made, set `retry_policy = "none"`.

The runtime gives every attempt an isolated work directory scoped by run,
process, impulse, and attempt. It writes the manifest, captures stdout/stderr,
validates `output/result.json` as a JSON object, and structurally canonicalizes
that object before committing the runtime result; the submitted JSON bytes are
not byte-preserved. Capabilities may declare `secret_handles`; a subprocess may resolve only those
handles for its concrete attempt. Package and journal metadata retain handle
names, never values. An undeclared handle fails package validation before
execution. Resolved values are scoped to that adapter environment and redacted
from operator-facing stdout/stderr streams; public graph inspection and
`explain` never include values.

Terminal execution metadata uses a provider-neutral provenance envelope:
package/path fingerprints, capability, adapter identity/version, stable
execution ID, attempt, timestamps, optional model/tool IDs, and validated
`usage`. Usage supports non-negative duration, input/output tokens, and cost
with a required unit. Aggregation preserves per-effector provenance and sums
compatible units; malformed usage fails the attempt closed. Packages without
secret or usage declarations retain the existing behavior.

An effector may declare vendor-neutral context continuity as `context_policy =
"fresh" | "resume" | "inherit"`. Resume keys derive from explicit
run/process/impulse identity and remain stable across physical retries;
`context_invalidation_digest` changes the key when material inputs change.
Inherit additionally requires a direct `context_source` whose durable process
is succeeded and has provenance. The subprocess manifest contains only the
resolved policy/key/source/digest. Fala stores no transcript or vendor session
ID; an adapter that cannot implement the declared policy must fail unsupported,
not silently start fresh. Omitting the policy preserves previous behavior.

Adapters never mutate a JournalPort,
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
