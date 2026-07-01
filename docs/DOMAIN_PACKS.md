# Domain Packs

Core Fala is domain-agnostic. Domain-specific objects should live in domain
packs that map their concepts onto Carrier runtime records.

Current packs:

- `fala.domain_packs.documents`
- `fala.domain_packs.splot`

Carrier-first example packs:

- `examples/domain-packs/signals`

Domain packs may provide:

- carrier builders and parsers
- observation helpers
- projection helpers
- package examples
- migration guidance from prior domain-specific models

Core runtime code must not depend on document-specific classes.
