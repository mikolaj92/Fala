# Splot Arbitration Domain Pack (Mojo)

`fala.domain_packs.splot` is a vocabulary pack, not the Splot arbitration
engine. It maps Splot concepts onto Fala's domain-agnostic records using pure
builders; scheduling, scoring, and arbitration remain in the separate Splot
0.3+ Mojo product hosted through a subprocess.

See the shared rules in [`DOMAIN_PACKS.md`](DOMAIN_PACKS.md), the subprocess
wire contract in [`ADAPTER_CONTRACTS.md`](ADAPTER_CONTRACTS.md), and the host
boundary in [`FALA_HOST_AND_COMPOSITION.md`](FALA_HOST_AND_COMPOSITION.md).

## Concepts and mappings

| Domain | Core mapping |
| --- | --- |
| Case | Impulse type `splot.arbitration_case` |
| Jurisdiction | Association kind `splot.jurisdiction` |
| Human review | Homeostat kind `splot.review` |
| Case summary | Projection name `splot.case:{claim_id}` |
| Decision report | Reaction kind `splot.decision_report` (package) |

Pack process vocabulary:

- `intake` — accept an arbitration case impulse;
- `jurisdiction` — record admissibility associations;
- `triage` — open/complete review homeostats;
- `award_projection` — maintain the case summary projection.

## Module and package

- Mojo module: `mojo/fala/domain_packs/splot.mojo`;
- helpers: `impulse_from_case`, `case_from_impulse`,
  `jurisdiction_association`, `review_homeostat`, `case_projection`;
- TOML package example:
  `examples/domain-packs/splot/fala-package.toml`.

## Optional Splot host

The integration example is [`../examples/splot-integration/`](../examples/splot-integration/).
It runs `Splot/tools/splot_step.sh` through `FALA_EFFECTOR_*`; the child writes
`output/result.json` with a decision envelope (`status`,
`selected_candidate_id`, and related fields). The child owns its own journal;
there is no Python Splot path or shared SQLite state.

```bash
mise exec -- pixi run splot-domain
mise exec -- pixi run splot-integration
```

Core must not import Splot product types. Domain packs depend only on
`fala.domain` records; arbitration policy remains in Splot.
