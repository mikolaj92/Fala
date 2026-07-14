# Domain Packs

Core Fala is domain-agnostic. Domain-specific objects should live in domain
packs that map their concepts onto Impulse runtime records.

Current packs:

- `fala.domain_packs.signals`
- `fala.domain_packs.splot`

Impulse-first examples:

- `examples/domain-packs/signals`

Domain packs may provide:

- impulse builders and parsers
- association helpers
- projection helpers
- package examples
- migration guidance from prior domain-specific models

Core runtime code must not depend on domain-specific classes from packs.
