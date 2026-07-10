# Splot Arbitration Domain Pack

`fala.domain_packs.splot` keeps arbitration behavior outside Fala core. Core
Fala provides impulses, commands, events, associations, homeostats, projections, and
SQLite persistence. Splot defines arbitration-specific meaning on top.

## Domain Concepts

- Impulse type: `splot.arbitration_case`
- Case input: `SplotArbitrationCase`
- Association: `splot.jurisdiction`
- Homeostat: `splot.review`
- Projection: `splot.case:{claim_id}`
- Reactions: case payload entries such as claim statements, awards, evidence
  bundles, or correspondence

## Process Semantics

The pack documents domain semantics without adding scheduler behavior to core:

- `intake`: accept arbitration case impulse and source reactions
- `jurisdiction`: record jurisdiction and admissibility associations
- `triage`: open or complete human review homeostats
- `award_projection`: maintain case summary projection for operators

## Boundary

Splot-specific rules belong in the domain pack. Fala core should not know about
claimants, respondents, admissibility, awards, arbitration rules, or Splot case
states. The pack uses public `AutonomousCorrelator` and `RuntimeBackend` APIs only.

Run the local example:

```bash
uv run python examples/domain-packs/splot/local_arbitration.py /tmp/splot.sqlite
```

Validate the fala package manifest:

```bash
uv run fala schema fala-package
uv run python - <<'PY'
from fala import load_fala_package_yaml
print(load_fala_package_yaml("examples/domain-packs/splot/fala-package.yaml").id)
PY
```
