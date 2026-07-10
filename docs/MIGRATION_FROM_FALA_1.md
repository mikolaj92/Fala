# Migration From Fala 1

Fala removes the document-workflow core. There are no compatibility aliases in
the core CLI or public schemas.

Migration mapping:

- `Document` -> `Impulse`
- `DocumentType` -> `ImpulseType`
- `DocumentRelation` -> `ImpulseRelation`
- `DocumentRegistry` -> fala package/domain pack definitions
- document workflow -> information correlation path

Document-specific behavior belongs in `fala.domain_packs.documents`. New package
YAML must use `impulse_types`, `impulse_relations`, associations, reactions,
capabilities, correlation_paths, and runtime config.

Recommended migration order:

1. Convert package YAML to the Impulse schema.
2. Move document-specific code into the document domain pack.
3. Replace document CLI usage with Impulse CLI commands.
4. Rebuild SQLite state with the Fala runtime schema.
5. Recreate tests around impulses, associations, reactions, events, homeostats, and
   projections.
