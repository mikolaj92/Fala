# Effector output schema: implemented validation boundaries

An effector may declare `output_schema` directly in a JSON or TOML package.
The loader retains it in expanded serialization and freezes it in the durable
process plan. The package host and `fala rehearse` use that same declaration.
For FEP/1 results the schema describes `values`, not the transport envelope or
host-added `adapter` telemetry. Direct native/fixture results are validated as
the domain object itself.

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

## Local proofs and remaining scope

`mise exec -- pixi run full-smoke` includes `output-contracts` and
`graph-rehearsal`. The former exercises valid variants, malformed payloads,
combinator semantics, nested mismatches, schema rejection, projection, durable
FEP completion and a real local FEP subprocess/consumer boundary. The latter
proves fixture-only execution, frozen output validation and terminal conditions.
These are contract/routing tests, not evidence that an agent's claims are true.

This implementation is **not completion of #237–#239**:

- Missing schemas still retain the historical `{}` default. Contract-first,
  explicit per-node opt-out and explicit legacy migration remain to implement.
- Finite variant enumeration, pre-execution edge/terminal coverage and static
  input/handoff compatibility analysis (#238) remain to implement.
- Automatic reporting of missing fixture variants and independent scenario
  coverage (#239) remain to implement. Adding an untested enum member does not
  yet make rehearsal fail merely because its fixture is absent.

Do not interpret a passing rehearsal as proof of complete graph coverage.
