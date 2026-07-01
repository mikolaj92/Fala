# Artifacts And References

The default artifact store is filesystem-backed and content-addressed by SHA-256.
SQLite stores artifact metadata, URI, media type, size, and content hash.

Artifact URIs use:

```text
fala-artifact://sha256/<digest>
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
- `ArtifactRef`
- `EventRef`
