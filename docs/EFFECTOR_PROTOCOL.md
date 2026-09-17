# Fala parent–child protocol

One closed object on the wire. Identity, status, and payload are always named.
Unknown protocol, kind, or field fails closed. JSON is canonicalized (sorted
keys, minimal separators, UTF-8) excluding `id`; identity is
`msg:sha256:<sha256(canonical-body)>`. Receivers recalculate it.

`protocol` is `fala`. There is one envelope, not a versioned family.

## `request` — parent → child

```json
{"config":{},"contract_id":"echo-output","contract_version":"1","from":"parent","id":"msg:sha256:…","job":"echo","kind":"request","payload":{"text":"hello"},"protocol":"fala","to":"echo"}
```

| Field | Meaning |
|---|---|
| `from` | who asks (`parent`) |
| `to` | which child (`process_id`) |
| `job` | which work (same id as the child unless a capability is later added) |
| `payload` | named input of this job |
| `config` | attempt, adapter, optional context — not the answer |
| `contract_id` | required; names the written contract both sides speak |
| `contract_version` | required; must accompany `contract_id` |

Both fields are nonempty (`fep.required`). A result must echo the request
pair (`fep.contract_mismatch`). The parent stamps the pair from the frozen
`output_schema` (`schema:sha256:<digest>` / `1`) when writing
`input/manifest.json`. Omitting the pair is unrepresentable.

The filesystem carrier is still `input/manifest.json`. That file **is** the
request.

## `result` — child → parent

```json
{"contract_id":"echo-output","contract_version":"1","from":"echo","id":"msg:sha256:…","job":"echo","kind":"result","payload":{"text":"hello"},"protocol":"fala","ref":"msg:sha256:…","status":"ok","to":"parent"}
```

| Field | Meaning |
|---|---|
| `from` | which child answered |
| `to` | who asked (`parent`) |
| `job` | same job as the request |
| `ref` | `id` of that request |
| `status` | `ok` or `error` |
| `payload` | named answer; `output_schema` describes this object |
| `contract_id` | echo of the request |
| `contract_version` | echo of the request |

`status` is the envelope, not the journal. Exit, timeout, bad JSON, and digest
mismatch are adapter failures and cannot impersonate a result. `wait` /
`route` belong in `payload`.

Construct a result from a request. Do not assemble a dict:

```python
from fala.protocol import Result, speak
from fala.sdk import load_manifest, write_result

request = load_manifest()
result = Result.from_request(request, payload={"text": "hello"})
write_result(result)
speak(result)  # bytes → parse → same object
```

## Conformance

`conformance/fala` is the shared golden corpus. Mojo `effector_protocol` and
Python `fala.protocol` consume the same vectors. A port needs UTF-8 JSON, sorted-key
compact serialization, SHA-256, and closed field checks. It needs no Fala
runtime.

| Layer | Corpus | Checker |
|---|---|---|
| L0 envelope | `request.valid.json`, `result.valid.json`, `negative.json` | `fala.conformance.check_message` |
| L1 dialogue | `dialogue.negative.json` (+ valid pair) | `fala.conformance.check_answer` / `assert_answers` |
| L2 payload | `payload.cases.json` | `fala.conformance.check_payload` |

Sibling effector packages validate without opening a journal:

```python
from fala.conformance import check_result, run_conformance
from fala.protocol import Result

request = ...  # load_manifest() / check_message(...)
result = Result.from_request(request, payload={"text": "hello"})
check_result(request, result, schema={"type": "object", "required": ["text"], "properties": {"text": {"type": "string"}}})

assert run_conformance(layers=["L0", "L1"]).get("ok")
```

`assert_answers` requires `ref == request.id`, flipped `from`/`to`, matching
`job`, and the contract pair. Prefer `Result.from_request` / `build_result`
over hand-built dicts.

Bare JSON, missing `protocol`, and unknown fields fail as
`adapter_invalid_result`. Native kernel returns the domain object; the parent
wraps it into this envelope before the journal and checks the wrap against the
request it just wrote. A contract mismatch on that wrap is the correlator's
(`blame=correlator`). Subprocess must return the envelope itself; a lying child
is `blame=effector`. The journal validates `payload` against `output_schema`.
