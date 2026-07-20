# Takt Cascade Domain Pack (Mojo)

`fala.domain_packs.takt` keeps cascade **vocabulary** outside Fala core.
Core provides impulses, commands, events, associations, homeostats, projections,
and journal sinks. This pack maps Takt-oriented names onto those records — pure
builders only; no fusion engine and no plant parser.

The **engine** (hierarchical tact → fusion → actuation / safety interlock)
lives in the separate **Takt** product (v0.2+, exclusive Mojo). Fala hosts it as
a subprocess effector; see Takt `docs/FALA_INTEGRATION.md` and
[`examples/takt-integration/`](../examples/takt-integration/) when present.

## Concepts (domain pack)

| Domain | Core mapping |
| --- | --- |
| Cascade evaluate/run request | Impulse type `takt.cascade_request` |
| Layer profile (`ProfilHomeostatyczny`) | Association kind `takt.plant_layer` |
| Fused `ErrorSignal` | Association kind `takt.error_signal` |
| Safety interlock | Homeostat kind `takt.safety_interlock` |
| Actuation footprint | Reaction kind `takt.actuation` (package) |
| Cascade summary | Projection name `takt.cascade:{impulse_id}` |

## Process semantics (pack vocabulary)

- `plant` — host builds hierarchical `plant_nodes` outside Takt  
- `cascade` — evaluate/run one or more tacts under layer profiles  
- `fusion` — record ErrorSignal associations  
- `interlock` — open safety interlock homeostat when fail-closed  
- `actuation` — reaction footprint; host applies to the world  
- `projection` — maintain `takt.cascade:{id}` for operators  

## Mojo module

- `mojo/fala/domain_packs/takt.mojo`
- Helpers: `impulse_from_cascade`, `cascade_from_impulse`,
  `plant_layer_association`, `error_signal_association`,
  `safety_interlock_homeostat`, `cascade_projection`, `process_semantics_json`

## Package example (vocabulary)

```bash
examples/domain-packs/takt/fala-package.toml
```

## Hosting the Takt engine (subprocess)

Sibling checkout (default):

```text
~/Developer/OSS/Fala
~/Developer/OSS/takt   # v0.2.0+
```

```bash
# Local Takt proof (from the Takt tree):
# TAKT_REQUEST_PATH=examples/fixtures/cascade_evaluate.request.json ./tools/takt_step.sh

mise exec -- pixi run takt-domain
```

Fala’s process host can run `takt/tools/takt_step.sh` with the effector boundary
(`FALA_EFFECTOR_*`). Takt writes `output/result.json` with outcome / events.
There is no Python Takt path.

## Proof

```bash
mise exec -- pixi run takt-domain
# included in extended-smoke
```

## Boundary

- Core must not import Takt **product** types; only optional pack consumers use
  `fala.domain_packs.takt`.
- Domain packs may depend on `fala.domain` records only.
- Cascade fusion / DFS plant scan lives in Takt, not in Fala core or this pack.
