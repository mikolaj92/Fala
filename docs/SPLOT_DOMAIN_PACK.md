# Splot Arbitration Domain Pack (Mojo)

`fala.domain_packs.splot` keeps arbitration **vocabulary** outside Fala core.
Core provides impulses, commands, events, associations, homeostats, projections,
and journal sinks. This pack maps Splot-oriented names onto those records — pure
builders only; no scheduler logic and no arbitration engine.

The **engine** (many signals → one decision) lives in the separate **Splot**
product (v0.3+, exclusive Mojo). Fala hosts it as a subprocess effector; see
[`examples/splot-integration/`](../examples/splot-integration/).

## Concepts (domain pack)

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

## Package example (vocabulary)

```bash
# Manifest (native TOML)
examples/domain-packs/splot/fala-package.toml
```

## Hosting the Splot engine (subprocess)

Sibling checkout (default):

```text
~/Developer/OSS/Fala
~/Developer/OSS/Splot   # v0.3.0+
```

```bash
# Override if needed:
# SPLOT_ROOT=/path/to/Splot

mise exec -- pixi run splot-integration
```

Fala’s process host runs `Splot/tools/splot_step.sh` with the effector boundary
(`FALA_EFFECTOR_*`). Splot writes `output/result.json` with a decision envelope
(`status`, `selected_candidate_id`, …). There is no Python Splot path.

## Proof

```bash
mise exec -- pixi run splot-domain        # vocabulary pack
mise exec -- pixi run splot-integration   # Fala host → Splot Mojo step
# splot-domain is also in extended-smoke
```

## Boundary

- Core must not import Splot **product** types; only optional pack consumers use
  `fala.domain_packs.splot`.
- Domain packs may depend on `fala.domain` records only.
- Arbitration scoring/policy lives in Splot, not in Fala core or this pack.
