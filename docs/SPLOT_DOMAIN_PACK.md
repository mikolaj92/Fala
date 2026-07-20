# Splot Arbitration Domain Pack (Mojo)

`fala.domain_packs.splot` keeps arbitration behavior **outside** Fala core.
Core provides impulses, commands, events, associations, homeostats, projections,
and journal sinks. Splot defines arbitration-specific meaning on top — pure
builders only; no scheduler logic.

## Concepts

| Domain | Core mapping |
| --- | --- |
| Case | Impulse type `splot.arbitration_case` |
| Jurisdiction | Association kind `splot.jurisdiction` |
| Human review | Homeostat kind `splot.review` |
| Case summary | Projection name `splot.case:{claim_id}` |
| Decision report | Reaction kind `splot.decision_report` (package) |

## Process semantics (pack vocabulary)

- `intake` — accept arbitration case impulse
- `jurisdiction` — record admissibility associations
- `triage` — open/complete review homeostats
- `award_projection` — case summary projection

## Mojo module

- `mojo/fala/domain_packs/splot.mojo`
- Helpers: `impulse_from_case`, `case_from_impulse`, `jurisdiction_association`,
  `review_homeostat`, `case_projection`

## Package example

```bash
# Manifest (native TOML)
examples/domain-packs/splot/fala-package.toml
```

## Proof

```bash
mise exec -- pixi run splot-domain
# also included in extended-smoke
```

## Boundary

Core must not import Splot types except via optional pack consumers. Domain
packs may depend on `fala.domain` records only.
