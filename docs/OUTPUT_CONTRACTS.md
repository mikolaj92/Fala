# Effector output schema: implemented validation boundaries

An effector declares `output_schema` in a JSON or TOML package. The loader
rejects a missing or empty schema, and it rejects `{ type = "object" }`:
that names no field, so the parent cannot observe the answer. A contract
names the fields (or a non-object type, `const`, `enum`, `required`,
`properties`, `items`, or a combinator). The Fala envelope carries identity;
`output_schema` is the contract for `payload`. The loader retains the contract in expanded
serialization and freezes it in the durable process plan. The package host
and `fala rehearse` use that same declaration.
For Fala results the schema describes `payload`, not the transport envelope or
host-added `adapter` telemetry. Native kernels still return the domain object;
the parent wraps it before the journal.

## Supported JSON Schema subset

Validation keywords: `type` (one type or a nonempty array), `const`, `enum`,
`required`, `properties`, boolean `additionalProperties`, schema-object `items`,
`minimum`, `maximum`, `minLength`, `maxLength`, `minItems`, `maxItems`, and
nonempty arrays of `oneOf`, `anyOf`, `allOf` schemas.

- `oneOf` requires exactly one matching branch.
- `anyOf` requires at least one matching branch.
- `allOf` requires every branch to match.
- Declarations are checked recursively before evaluating alternatives: a valid
  alternative cannot hide an unsupported keyword or malformed branch.
- String lengths count Unicode code points.
- `$schema`, `$id`, `$comment`, `title`, `description`, `deprecated`, `readOnly`,
  and `writeOnly` are annotations, not constraints or reference resolution.
- Other keywords, including `$ref`, `pattern`, and schema-valued
  `additionalProperties`, are rejected with a schema pointer. This intentionally
  rejects declarations previously accepted without enforcement; replace them
  with the supported subset rather than relying on ignored constraints.

The journal and conduction share the payload evaluator. Projection retains
properties from applicable alternative branches as well as the base schema;
otherwise a valid variant's required data could disappear during conduction.
The production host and rehearsal share terminal selection, including `when`,
output schema validation, missing matches and ambiguous matches.

## Example

```toml
[[correlation_paths.effectors]]
id = "decide"
output_schema = { type = "object", required = ["route"], properties = { route = { enum = ["ready", "wait"] } }, oneOf = [{ properties = { route = { const = "ready" }, artifact = { type = "string" } }, required = ["artifact"] }, { properties = { route = { const = "wait" }, reason = { type = "string" } }, required = ["reason"] }] }
adapter = { kind = "subprocess", command = ["decision-program"] }
```

Run an existing complete package with fixture outcomes, without executing its
production adapters:

```sh
fala rehearse --package package.toml --fixture acceptance.json --path-id ship \
  --journal rehearsal.sqlite --report rehearsal.json
```

```json
{
  "effectors": {
    "decide": [{"kind":"result","output":{"route":"ready","artifact":"local.txt"}}]
  },
  "assert": {"terminal":"done","attempts":{"decide":1}}
}
```

This fixture requires a corresponding `done` terminal in the package. A result
`{"route":"ready","reason":"wrong variant"}` is rejected. Outcome arrays
represent attempts, not independent variant-coverage scenarios.

## Local proofs

`mise exec -- pixi run full-smoke` includes `output-contracts` and
`graph-rehearsal`. The former exercises valid variants, malformed payloads,
combinator semantics, nested mismatches, schema rejection, projection, durable
completion and a real local subprocess/consumer boundary. The latter
proves fixture-only execution, frozen output validation, terminal conditions,
and fail-closed missing fixture variants. These are contract/routing tests, not
evidence that an agent's claims are true.

Current behavior:

- Every effector declares a non-empty `output_schema` that names the answer.
  `{ type = "object" }` is not a contract. `output_contract_ref` may label it;
  it does not replace it.
- The parent does not inspect the child. A result that does not match the
  declared schema is rejected. There is no side exit and no `contract_mode`.
- Graph preflight reports missing finite output-variant coverage before any
  adapter runs (`coverage_guaranteed` / `unverified`).
- Rehearsal fails closed when a finite declared variant has no fixture.

A passing rehearsal of supplied scenarios is not proof that an agent told the
truth, only that the declared contract and routing held for those fixtures.
