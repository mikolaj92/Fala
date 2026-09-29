# Reactions and References

The default reaction store is `FileReactionStore`, a filesystem
content-addressed store. It hashes the exact bytes with SHA-256 and stores
blobs at:

```text
<reaction-root>/blobs/sha256/<first2>/<digest>
```

Writes use a temporary sibling followed by POSIX rename. Existing blobs are
revalidated against their digest before reuse. The journal stores reaction
metadata and references (`uri`, media type, size, and `content_hash`);
reaction bytes remain outside the journal.

Reaction URIs are exactly:

```text
fala-reaction://sha256/<64 lowercase hexadecimal characters>
```

Uppercase hexadecimal URIs are noncanonical and rejected. `content_hash`
values using the `sha256:` form must identify the same digest as a CAS URI
when both are present.

`fala gc` is optional SQLite maintenance. It removes only local CAS blobs
whose digests are unreferenced by the SQLite runtime, protecting references
from every run even when `--run-id` is supplied. The SQLite scan and
filesystem deletion are separate operations.

## Cross-journal envelope references

Typed references identify foreign journal records in an explicit envelope.
They are not a peer directory:

- `RuntimeRef(id, uri, metadata)` — a journal/runtime
- `RunRef(runtime, run_id)` — a run in that journal
- `ReactionRef(id, kind, uri, metadata)` — a reaction
- `EventRef(runtime, run_id, event_id, sequence)` — an event

Parent and child Falas keep separate journals. See
[`FALA_HOST_AND_COMPOSITION.md`](FALA_HOST_AND_COMPOSITION.md).
