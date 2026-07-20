# SQLite Backend

SQLite is the bundled **reference journal sink** for Fala's event-first core
(see [`EVENT_STREAM_CORE.md`](EVENT_STREAM_CORE.md)). The `Correlator` /
`SqliteJournal` path stores:

- runs, impulses, impulse types, and impulse relations
- associations, reaction metadata, processes, homeostats, and projections
- append-only runtime commands and runtime events
- bridge inbox/outbox deliveries
- runtime pools and delegation policies
- schema migration state

The backend initializes SQLite with WAL journal mode, foreign keys, and a busy
timeout. Reaction bytes are not stored in SQLite by default; SQLite stores refs
and metadata.

Run creation stores the run row, `run.create` command, and `run.created` event in
one SQLite transaction.
Run status changes store the `run.status.set` or `run.cancel` command, event,
and run status update in one SQLite transaction.
Impulse acceptance stores the impulse row, `impulse.accept` command, and
`impulse.accepted` event in one SQLite transaction.
Impulse type registration, impulse relation recording, association recording,
reaction recording, process scheduling, and process status transitions also
commit their runtime command, event, and state change together.
Homeostat creation and terminal transitions are committed the same way.
Projection save and rebuild commands commit their projection writes in the same
transaction.
Bridge outbox enqueue/deliver and inbox import commands commit their local
delivery rows with the command and event in one transaction.

Runtime commands and events are guarded by SQLite triggers that reject direct
updates and deletes. New runtime facts must be appended through command
submission.

Run-scoped writes reject unknown run ids before storing runtime state.

The backend is local-first and requires no Redis, Postgres, queue broker, web
server, Docker, or external service.
