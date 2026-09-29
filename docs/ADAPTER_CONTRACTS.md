# Adapter Contracts

Fala's default effector boundary is a local subprocess. All adapters execute
one claimed process; the runtime owns process state, events, reaction
metadata, and journal writes.

- `subprocess`: argv list; primary child boundary.
- `native_function`: registered in-process Mojo callable.
- `manual_homeostat`: durable operator wait.
- `child_path`: package-authored nested path. The loader stores the spec;
  `host_run_package` compiles it to argv + `python/fala/child_path.py`. It
  is not a fourth process-host transport.

Unknown adapter kinds fail closed. `runtime_ref` is not an adapter field.
Nested Fala uses `subprocess` / `child_path` and a separate journal.

## Subprocess wire

Each attempt receives `input/manifest.json` and writes `output/result.json`.
The manifest **is** a Fala `request` (see
[`EFFECTOR_PROTOCOL.md`](EFFECTOR_PROTOCOL.md)):

- `from` / `to` / `job` / `id`: who asks, which child, which work, this message
- `payload`: named input
- `config`: `attempt`, `max_attempts`, optional `impulse_id` / `context`,
  and adapter metadata

Retries preserve `execution_id` and increment `attempt`. Automatic retry is
at-least-once for external effects. Deduplicate by `execution_id` before an
external effect, or set `retry_policy = "none"`.

Every attempt gets an isolated work directory. The runtime writes the
manifest, captures stdout/stderr, validates `output/result.json` as a JSON
object, and structurally canonicalizes it before commit. Capabilities may
declare `secret_handles`; a subprocess may resolve only those handles for
its attempt. Package and journal metadata retain handle names, never values.
Resolved values are redacted from operator-facing stdout/stderr. Public
graph inspection and `explain` never include values.

Terminal execution metadata uses a provider-neutral provenance envelope:
package/path fingerprints, capability, adapter identity/version, stable
execution ID, attempt, timestamps, optional model/tool IDs, and validated
`usage`. Usage supports non-negative duration, input/output tokens, and cost
with a required unit. Malformed usage fails the attempt closed.

An effector may declare `context_policy = "fresh" | "resume" | "inherit"`.
Resume keys derive from run/process/impulse identity and stay stable across
physical retries; `context_invalidation_digest` changes the key when
material inputs change. Inherit requires a direct `context_source` whose
durable process succeeded and has provenance. The manifest contains only the
resolved policy/key/source/digest. Fala stores no transcript or vendor
session ID. An adapter that cannot implement the declared policy must fail
unsupported, not silently start fresh.

Adapters never mutate a JournalPort, NativeJournal, SQLite database, or
other Fala journal directly.

## `child_path`

Authored without `command` / `ref` / `env`. Required: `package_ref`,
`path_id`, `journal_root` (a directory, not a journal file),
`input_mapping`, `terminal_mapping`, positive `lifetime_seconds`, and
`retention` (`keep` or `delete_on_success`). Host-owned `FALA_*`
environment names cannot be authored.

```toml
adapter = {
  kind = "child_path",
  package_ref = "child.fala-package.toml",
  path_id = "ship",
  journal_root = ".fala/children",
  input_mapping = { ticket = "ticket" },
  terminal_mapping = { delivered = "ok" },
  lifetime_seconds = 30,
  retention = "keep",
}
```

The child run ID and journal filename are a stable digest of the parent
run/process identity. Typed `path_result` returns through `result.json`,
with `child_ref = {journal, run_id, path_digest, terminal}` in parent
values/metadata. `delete_on_success` unlinks the child SQLite file (and
WAL/SHM sidecars) after that typed result. Native CLI dispatch does not
compile this kind; only the Python host does.

Python subprocesses may use `fala.sdk` to read `FALA_EFFECTOR_MANIFEST` and
write `FALA_EFFECTOR_OUTPUT_DIR/result.json`. See
[`PROCESS_RUNTIME.md`](PROCESS_RUNTIME.md),
[`RUNTIME_SEMANTICS.md`](RUNTIME_SEMANTICS.md), and
[`SECURITY.md`](SECURITY.md).
