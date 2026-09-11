# Fala parent–child protocol

One closed object on the wire. Identity, status, and payload are always named.
Unknown protocol, kind, or field fails closed. JSON is canonicalized (sorted
keys, minimal separators, UTF-8) excluding `id`; identity is
`msg:sha256:<sha256(canonical-body)>`. Receivers recalculate it.

`protocol` is `fala`. There is one envelope, not a versioned family.

## `request` — parent → child

```json
{"config":{},"from":"parent","id":"msg:sha256:…","job":"echo","kind":"request","payload":{"text":"hello"},"protocol":"fala","to":"echo"}
```

| Field | Meaning |
|---|---|
| `from` | who asks (`parent`) |
| `to` | which child (`process_id`) |
| `job` | which work (same id as the child unless a capability is later added) |
| `payload` | named input of this job |
| `config` | attempt, adapter, optional context — not the answer |

The filesystem carrier is still `input/manifest.json`. That file **is** the
request.

## `result` — child → parent

```json
{"from":"echo","id":"msg:sha256:…","job":"echo","kind":"result","payload":{"text":"hello"},"protocol":"fala","ref":"msg:sha256:…","status":"ok","to":"parent"}
```

| Field | Meaning |
|---|---|
| `from` | which child answered |
| `to` | who asked (`parent`) |
| `job` | same job as the request |
| `ref` | `id` of that request |
| `status` | `ok` or `error` |
| `payload` | named answer; `output_schema` describes this object |

`status` is the envelope, not the journal. Exit, timeout, bad JSON, and digest
mismatch are adapter failures and cannot impersonate a result. `wait` /
`route` belong in `payload`.

Construct a result from a request. Do not assemble a dict:

```python
from fala.protocol import Result, speak
from fala.sdk import load_manifest, write_result

request = load_manifest()
result = Result.ok(sender=request.recipient, job=request.job, ref=request.id, payload={"text": "hello"})
write_result(result)
speak(result)  # bytes → parse → same object
```

## Conformance

`conformance/fala` is the shared golden corpus. Mojo `effector_protocol` and
Python `fala.protocol` consume the same vectors. A port needs UTF-8 JSON, sorted-key
compact serialization, SHA-256, and closed field checks. It needs no Fala
runtime.

Bare JSON, missing `protocol`, and unknown fields fail as
`adapter_invalid_result`. Native kernel returns the domain object; the parent
wraps it into this envelope before the journal. Subprocess must return the
envelope itself. The journal validates `payload` against `output_schema`.
