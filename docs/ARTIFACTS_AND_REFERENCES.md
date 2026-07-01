# Artifacts And References

The default artifact store is filesystem-backed and content-addressed by SHA-256.
SQLite stores artifact metadata, URI, media type, size, and content hash.

Artifact URIs use:

```text
fala-artifact://sha256/<digest>
```

`fala gc` removes only blobs not referenced by any run in the SQLite runtime.
This protects shared blobs even when `--run-id` is supplied.

Cross-runtime references use:

- `RuntimeRef`
- `RunRef`
- `ArtifactRef`
- `EventRef`
