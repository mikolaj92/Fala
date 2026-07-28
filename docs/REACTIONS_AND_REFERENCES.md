# Reactions and References

The default reaction store is `FileReactionStore`, a filesystem content-addressed
store. It hashes the exact bytes with SHA-256 and stores blobs at:

```text
<reaction-root>/blobs/sha256/<first2>/<digest>
```

Writes use a temporary sibling followed by POSIX rename. Existing blobs are
revalidated against their digest before reuse. The journal stores reaction
metadata and references (`uri`, media type, size, and `content_hash`); reaction
bytes remain outside the journal.

Reaction URIs are exactly:

```text
fala-reaction://sha256/<64 lowercase hexadecimal characters>
```

The digest portion is strict lowercase; uppercase hexadecimal URIs are
noncanonical and rejected. `content_hash` values using the `sha256:` form must
identify the same digest as a CAS URI when both are present.

`fala gc` is optional SQLite maintenance, not an archive command. It removes
only local CAS blobs whose digests are unreferenced by the SQLite runtime,
protecting references from every run even when `--run-id` is supplied. The
SQLite row/reference scan and filesystem blob deletion are separate operations;
they are not one cross-store transaction.

## Cross-journal envelope references

Typed references identify foreign journal records in an explicit envelope; they
are not a peer directory or discovery mechanism:

- `RuntimeRef(id, uri, metadata)` identifies a journal/runtime;
- `RunRef(runtime, run_id)` identifies a run in that journal;
- `ReactionRef(id, kind, uri, metadata)` identifies a reaction;
- `EventRef(runtime, run_id, event_id, sequence)` identifies an event.

Parent and child Falas keep separate journals. See
[`CONCEPTUAL_MODEL.md`](CONCEPTUAL_MODEL.md) for reaction placement,
[`FALA_HOST_AND_COMPOSITION.md`](FALA_HOST_AND_COMPOSITION.md) for envelope
handoff without a mesh, and [`EVENTS_AND_REPLAY.md`](EVENTS_AND_REPLAY.md) for
replay and recorded reaction identity.
