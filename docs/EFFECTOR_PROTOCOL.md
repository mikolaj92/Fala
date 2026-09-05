# Fala Effector Protocol v1

FEP/1 is the transport-neutral boundary between one materialized graph role and
one executor. Filesystem, stdio, HTTP, or another carrier transport the same
canonical document; they do not alter its meaning.

## Canonicalization and identity

`protocol` is `fala-effector/1`. Messages are closed objects: unknown protocol
versions, kinds, and fields fail closed. JSON is canonicalized (sorted keys,
minimal separators, UTF-8), excluding `message_id`; identity is
`msg:sha256:<sha256(canonical-body)>`. Receivers recalculate it. Formatting and
input key order therefore cannot change identity. A protocol version changes
only through a new protocol identifier; there is no permissive downgrade.

## `effector.request`

Required fields are `protocol`, `message_kind`, `message_id`, `run_id`,
`process_id`, `execution_id`, positive `attempt`, `impulse_id`,
`process_fingerprint`, `path_digest`, `capability`, object `input`, object
`config`, and `output_contract_ref` (schema reference or digest).
`execution_id` is the stable semantic execution while `attempt` is a physical
try.

```json
{"attempt":1,"capability":"draft.create","config":{},"execution_id":"run:p","impulse_id":"i","input":{"title":"A"},"message_id":"msg:sha256:…","message_kind":"effector.request","output_contract_ref":"schema:sha256:…","path_digest":"sha256:…","process_fingerprint":"sha256:…","process_id":"p","protocol":"fala-effector/1","run_id":"run"}
```

## `effector.result`

Required fields are `protocol`, `message_kind`, `message_id`, `request_id`, a
single `causation.request_id` equal to it, `execution_id`, positive `attempt`,
object `values`, arrays `associations` and `reactions`, object `metadata`, array
`evidence_refs`, and optional-neutral objects `provenance` and `usage`. Large
payloads belong in canonical reaction/evidence references, not inline.

```json
{"associations":[],"attempt":1,"causation":{"request_id":"msg:sha256:…"},"evidence_refs":["evidence:sha256:…"],"execution_id":"run:p","message_id":"msg:sha256:…","message_kind":"effector.result","metadata":{},"protocol":"fala-effector/1","provenance":{},"reactions":[],"request_id":"msg:sha256:…","usage":{},"values":{"id":"draft-1"}}
```

A valid result is a contract terminal candidate; process exit, timeout, missing
file, malformed JSON, digest mismatch, causation mismatch, or output-shape
failure is an adapter/transport failure and cannot impersonate that terminal.
World confirmation remains a separate observe/confirm operation.

## Filesystem compatibility

The default carrier remains `input/manifest.json` to `output/result.json`.
New executors read/write FEP/1 directly. Existing subprocess packages using the
legacy unversioned result object remain accepted during migration; they should
adopt FEP/1 and preserve `request_id` before a future package schema makes it
mandatory. Journals may persist message and causation IDs as metadata; these
IDs never include filesystem paths or transport identity.
