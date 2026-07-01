# Splot Arbitration Domain Pack

`fala.domain_packs.splot` keeps arbitration behavior outside Fala core. Core
Fala provides carriers, commands, events, observations, gates, projections, and
SQLite persistence. Splot defines arbitration-specific meaning on top.

## Domain Concepts

- Carrier type: `splot.arbitration_case`
- Case input: `SplotArbitrationCase`
- Observation: `splot.jurisdiction`
- Gate: `splot.review`
- Projection: `splot.case:{claim_id}`
- Artifacts: case payload entries such as claim statements, awards, evidence
  bundles, or correspondence

## Process Semantics

The pack documents domain semantics without adding scheduler behavior to core:

- `intake`: accept arbitration case carrier and source artifacts
- `jurisdiction`: record jurisdiction and admissibility observations
- `triage`: open or complete human review gates
- `award_projection`: maintain case summary projection for operators

## Boundary

Splot-specific rules belong in the domain pack. Fala core should not know about
claimants, respondents, admissibility, awards, arbitration rules, or Splot case
states. The pack uses public `FalaRuntime` and `RuntimeBackend` APIs only.

Run the local example:

```bash
uv run python examples/domain-packs/splot/local_arbitration.py /tmp/splot.sqlite
```
