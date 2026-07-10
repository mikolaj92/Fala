# Reactions And References

The default reaction store is filesystem-backed and content-addressed by SHA-256.
SQLite stores reaction metadata, URI, media type, size, and content hash.
Existing filesystem blobs are verified against their digest before reuse.

Reaction URIs use:

```text
fala-reaction://sha256/<digest>
```

`fala gc` removes only blobs not referenced by any run in the SQLite runtime.
This protects shared blobs even when `--run-id` is supplied.

`fala archive-run --retention-days N` records archive retention metadata in the
portable archive manifest.
`fala archive-gc --archive-root <dir>` deletes expired archive bundles whose
manifest `retain_until` has passed.

Cross-runtime references use:

- `RuntimeRef`
- `RunRef`
- `ReactionRef`
- `EventRef`
