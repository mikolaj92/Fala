# Effector output schema

An effector declares `output_schema` in a JSON or TOML package. The loader
rejects a missing or empty schema, and it rejects `{ type = "object" }`:
that names no field. A contract names the fields (or a non-object type,
`const`, `enum`, `required`, `properties`, `items`, or a combinator). The
Fala envelope carries identity; `output_schema` is the contract for
`payload`. Native kernels still return the domain object; the parent wraps
it before the journal.

## Supported JSON Schema subset

Validation keywords: `type` (one type or a nonempty array), `const`, `enum`,
`required`, `properties`, boolean `additionalProperties`, schema-object
`items`, `minimum`, `maximum`, `minLength`, `maxLength`, `minItems`,
`maxItems`, and nonempty arrays of `oneOf`, `anyOf`, `allOf` schemas.

- `oneOf` requires exactly one matching branch.
- `anyOf` requires at least one matching branch.
- `allOf` requires every branch to match.
- Declarations are checked recursively before evaluating alternatives.
- String lengths count Unicode code points.
- `$schema`, `$id`, `$comment`, `title`, `description`, `deprecated`,
  `readOnly`, and `writeOnly` are annotations, not constraints.
- Other keywords, including `$ref`, `pattern`, and schema-valued
  `additionalProperties`, are rejected with a schema pointer.

The journal and conduction share the payload evaluator. Projection retains
properties from applicable alternative branches as well as the base schema.
Production and rehearsal share terminal selection, including `when`.

## Example

```toml
[[correlation_paths.effectors]]
id = "decide"
output_schema = { type = "object", required = ["route"], properties = { route = { enum = ["ready", "wait"] } }, oneOf = [{ properties = { route = { const = "ready" }, artifact = { type = "string" } }, required = ["artifact"] }, { properties = { route = { const = "wait" }, reason = { type = "string" } }, required = ["reason"] }] }
adapter = { kind = "subprocess", command = ["decision-program"] }
```

Run a package with fixture outcomes, without production adapters:

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

Outcome arrays represent attempts, not independent variant-coverage
scenarios. Graph preflight reports missing finite output-variant coverage
before any adapter runs (`coverage_guaranteed` / `unverified`). Rehearsal
fails closed when a finite declared variant has no fixture. A passing
rehearsal is not proof that an agent told the truth.

`mise exec -- pixi run full-smoke` includes `output-contracts` and
`graph-rehearsal`.
