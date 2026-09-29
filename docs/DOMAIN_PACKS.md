# Domain Packs

A domain pack is a small Mojo vocabulary layer. It maps product names onto
core Impulse, Association, Reaction, Homeostat, and Projection records. Packs
do not change the ontology or implement product engines.

| Pack | Module | Proof |
| --- | --- | --- |
| Splot | `mojo/fala/domain_packs/splot.mojo` | `pixi run splot-domain` |
| Signals | `mojo/fala/domain_packs/signals.mojo` | `pixi run signals-domain` |
| Takt | `mojo/fala/domain_packs/takt.mojo` | `pixi run takt-domain` |

Helpers and TOML examples live under `examples/domain-packs/`. Packs depend
only on public `fala.domain` records. Core (`mojo/fala` organ, JournalPort,
driver, process host) stays domain-agnostic.

Splot 0.3+ and Takt 0.2+ engines are separate products. Fala hosts them as
subprocess children with a separate child journal:

- Splot: `examples/splot-integration/`
- Takt: `examples/takt-integration/`

## Splot mapping

| Domain | Core |
| --- | --- |
| Case | Impulse type `splot.arbitration_case` |
| Jurisdiction | Association kind `splot.jurisdiction` |
| Human review | Homeostat kind `splot.review` |
| Case summary | Projection name `splot.case:{claim_id}` |
| Decision report | Reaction kind `splot.decision_report` |

Helpers: `impulse_from_case`, `case_from_impulse`,
`jurisdiction_association`, `review_homeostat`, `case_projection`.

```bash
mise exec -- pixi run splot-domain
mise exec -- pixi run splot-integration
```

## Takt mapping

| Domain | Core |
| --- | --- |
| Cascade evaluate/run request | Impulse type `takt.cascade_request` |
| Layer profile | Association kind `takt.plant_layer` |
| Fused `ErrorSignal` | Association kind `takt.error_signal` |
| Safety interlock | Homeostat kind `takt.safety_interlock` |
| Actuation footprint | Reaction kind `takt.actuation` |
| Cascade summary | Projection name `takt.cascade:{impulse_id}` |

Helpers: `impulse_from_cascade`, `cascade_from_impulse`,
`plant_layer_association`, `error_signal_association`,
`safety_interlock_homeostat`, `cascade_projection`,
`process_semantics_json`.

```bash
mise exec -- pixi run takt-domain
```
