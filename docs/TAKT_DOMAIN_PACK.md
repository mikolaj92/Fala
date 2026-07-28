# Takt Cascade Domain Pack (Mojo)

`fala.domain_packs.takt` is a vocabulary pack, not the Takt cascade engine. It
maps Takt names onto Fala's domain records with pure builders; fusion, plant
parsing, actuation, and safety policy remain in the separate Takt 0.2+ Mojo
product hosted through a subprocess.

See the shared rules in [`DOMAIN_PACKS.md`](DOMAIN_PACKS.md), the subprocess
wire contract in [`ADAPTER_CONTRACTS.md`](ADAPTER_CONTRACTS.md), and the host
boundary in [`FALA_HOST_AND_COMPOSITION.md`](FALA_HOST_AND_COMPOSITION.md).

## Concepts and mappings

| Domain | Core mapping |
| --- | --- |
| Cascade evaluate/run request | Impulse type `takt.cascade_request` |
| Layer profile (`ProfilHomeostatyczny`) | Association kind `takt.plant_layer` |
| Fused `ErrorSignal` | Association kind `takt.error_signal` |
| Safety interlock | Homeostat kind `takt.safety_interlock` |
| Actuation footprint | Reaction kind `takt.actuation` (package) |
| Cascade summary | Projection name `takt.cascade:{impulse_id}` |

Pack process vocabulary:

- `plant` — host builds hierarchical `plant_nodes` outside Takt;
- `cascade` — evaluate/run tacts under layer profiles;
- `fusion` — record `ErrorSignal` associations;
- `interlock` — open a fail-closed safety-interlock homeostat;
- `actuation` — reaction footprint applied by the host;
- `projection` — maintain `takt.cascade:{impulse_id}` for operators.

## Module and package

- Mojo module: `mojo/fala/domain_packs/takt.mojo`;
- helpers: `impulse_from_cascade`, `cascade_from_impulse`,
  `plant_layer_association`, `error_signal_association`,
  `safety_interlock_homeostat`, `cascade_projection`,
  `process_semantics_json`;
- TOML package example:
  `examples/domain-packs/takt/fala-package.toml`.

## Optional Takt host

The local integration example is [`../examples/takt-integration/`](../examples/takt-integration/).
It runs `takt/tools/takt_step.sh` through `FALA_EFFECTOR_*`; the child writes
`output/result.json` with outcome and events. The child owns its own journal;
there is no Python Takt path or shared SQLite state.

```bash
mise exec -- pixi run takt-domain
```

Core must not import Takt product types. Domain packs depend only on
`fala.domain` records; cascade fusion and plant scanning remain in Takt.
