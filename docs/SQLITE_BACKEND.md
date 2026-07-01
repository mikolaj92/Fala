# SQLite Backend

SQLite is the bundled reference runtime backend. It stores:

- runs, carriers, carrier types, and carrier relations
- observations, artifact metadata, processes, gates, and projections
- append-only runtime commands and runtime events
- bridge inbox/outbox deliveries
- runtime pools and delegation policies
- schema migration state

The backend initializes SQLite with WAL journal mode, foreign keys, and a busy
timeout. Artifact bytes are not stored in SQLite by default; SQLite stores refs
and metadata.

Runtime commands and events are guarded by SQLite triggers that reject direct
updates and deletes. New runtime facts must be appended through command
submission.

Run-scoped writes reject unknown run ids before storing runtime state.

The backend is local-first and requires no Redis, Postgres, queue broker, web
server, Docker, or external service.
