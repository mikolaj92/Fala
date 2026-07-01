# SQLite Backend

SQLite is the bundled reference runtime backend. It stores:

- runs, carriers, carrier types, and carrier relations
- observations, artifact metadata, processes, gates, and projections
- runtime commands and append-only runtime events
- bridge inbox/outbox deliveries
- runtime pools and delegation policies
- schema migration state

The backend initializes SQLite with WAL journal mode, foreign keys, and a busy
timeout. Artifact bytes are not stored in SQLite by default; SQLite stores refs
and metadata.

The backend is local-first and requires no Redis, Postgres, queue broker, web
server, Docker, or external service.
