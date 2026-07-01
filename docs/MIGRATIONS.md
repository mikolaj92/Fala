# Migrations

Fala uses explicit version boundaries. Runtime data should never rely on
implicit legacy aliases or fallback parsing.

Migration kinds:

- SQLite schema migration: tracked in `schema_migrations` with the
  `runtime_backend` id.
- Package schema migration: package YAML declares `version: "2"` and must be
  parsed through the Carrier package model.
- Event payload migration: events carry `schema_version`; projections must
  tolerate known event schema versions or fail loudly.
- Artifact kind migration: artifact kinds belong in Carrier package/domain pack
  definitions and should use new kind ids when semantics change.
- Domain pack migration: domain packs own their domain-specific mapping changes.
- Report/profile migration: exported bundles declare their archive/report format.

Policy:

1. Additive SQLite changes get a new runtime backend schema version.
2. Breaking package changes get a new package schema version.
3. Breaking event payload changes get a new event `schema_version`.
4. Artifacts are immutable; changed artifact semantics require a new artifact
   kind or metadata schema version.
5. Domain packs may provide one-way migration helpers, but core must stay
   Carrier-first.
6. Unknown versions fail validation instead of silently falling back.
