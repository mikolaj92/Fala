# Migration From Fala 1

Fala 2 removes the document-workflow core. There are no compatibility aliases in
the core CLI or public schemas.

Migration mapping:

- `Document` -> `Carrier`
- `DocumentType` -> `CarrierType`
- `DocumentRelation` -> `CarrierRelation`
- `DocumentRegistry` -> carrier package/domain pack definitions
- document workflow -> information flow

Document-specific behavior belongs in `fala.domain_packs.documents`. New package
YAML must use `carrier_types`, `carrier_relations`, observations, artifacts,
capabilities, flows, and runtime config.

Recommended migration order:

1. Convert package YAML to Carrier v2 schema.
2. Move document-specific code into the document domain pack.
3. Replace document CLI usage with Carrier CLI commands.
4. Rebuild SQLite state with the Fala 2 runtime schema.
5. Recreate tests around carriers, observations, artifacts, events, gates, and
   projections.
