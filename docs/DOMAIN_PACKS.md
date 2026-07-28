# Domain Packs

Fala core is an autonomous, domain-agnostic Correlator. A domain pack is a
small Mojo vocabulary layer that maps a product's concepts onto core Impulse,
Association, Reaction, Homeostat, and Projection records. Packs do not change
the core ontology or implement product engines.

## Vocabulary packs

| Pack | Module | Proof |
| --- | --- | --- |
| Splot vocabulary | `mojo/fala/domain_packs/splot.mojo` | `pixi run splot-domain` |
| Signals vocabulary | `mojo/fala/domain_packs/signals.mojo` | `pixi run signals-domain` |
| Takt vocabulary | `mojo/fala/domain_packs/takt.mojo` | `pixi run takt-domain` |

Vocabulary packs provide pure builders/parsers, association and homeostat
helpers, projection helpers, and TOML package examples under
`examples/domain-packs/`.

## Sibling product hosts

Splot and Takt engines are separate Mojo products. Fala hosts them through the
local subprocess adapter and a separate child journal; the pack is not the
engine. Host/integration examples are:

- Splot: `examples/splot-integration/` (Splot 0.3+);
- Takt: `examples/takt-integration/` (Takt 0.2+).

The host chooses the sibling checkout and owns process execution. There is no
Python engine path, fleet identity, or shared journal.

## Shared rules

- Core runtime (`mojo/fala` organ, JournalPort, driver, and process host) stays
  domain-agnostic.
- Packs depend only on public `fala.domain` records.
- Product engines remain exclusive Mojo and own their scoring, fusion, or
  policy logic.
- Package manifests use TOML and canonical JSON; pack mappings must preserve
  the event-first runtime contracts.

See [`CONCEPTUAL_MODEL.md`](CONCEPTUAL_MODEL.md),
[`ADAPTER_CONTRACTS.md`](ADAPTER_CONTRACTS.md), and
[`FALA_HOST_AND_COMPOSITION.md`](FALA_HOST_AND_COMPOSITION.md) for the common
ontology, subprocess boundary, and child-journal rules. See
[`SPLOT_DOMAIN_PACK.md`](SPLOT_DOMAIN_PACK.md) and
[`TAKT_DOMAIN_PACK.md`](TAKT_DOMAIN_PACK.md) for pack-specific mappings.
