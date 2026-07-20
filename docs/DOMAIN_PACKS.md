# Domain Packs

Core Fala is domain-agnostic. Domain-specific objects live in **domain packs**
that map concepts onto Impulse runtime records.

## Packs (Mojo)

| Pack | Module | Proof |
| --- | --- | --- |
| **Splot** (vocabulary) | `mojo/fala/domain_packs/splot.mojo` | `pixi run splot-domain` |
| **Splot engine** (host) | sibling product `Splot` 0.3+ via subprocess | `pixi run splot-integration` |
| **Signals** (vocabulary) | `mojo/fala/domain_packs/signals.mojo` | `pixi run signals-domain` |
| Multi-organ sketch | `examples/multi-organ/` | `pixi run multi-organ-example` |

## What packs provide

- impulse builders / parsers
- association helpers
- homeostat helpers
- projection helpers
- package manifests under `examples/domain-packs/`

## Rules

- Core runtime (`mojo/fala` organ, journal, driver, host) must **not** depend on
  domain-specific classes for its identity.
- Packs depend on public domain records only.
- Product engine remains exclusive Mojo; packs are also Mojo.

See [`SPLOT_DOMAIN_PACK.md`](SPLOT_DOMAIN_PACK.md).
