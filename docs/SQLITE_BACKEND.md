# SQLite Backend

SQLite is Fala's bundled **reference journal sink** for the event-first core.
The public sink is `SqliteJournalPort`; its native engine is `NativeJournal`.
The port implements JournalPort while the engine provides the SQLite-backed
domain operations. See [`JOURNALPORT_CORE_PATH.md`](JOURNALPORT_CORE_PATH.md)
and [`RUNTIME_SEMANTICS.md`](RUNTIME_SEMANTICS.md).

The backend stores:

- runs, impulses, impulse types, and impulse relations;
- associations, reaction metadata, processes, homeostats, and projections;
- append-only runtime commands and runtime events;
- optional bridge inbox/outbox deliveries;
- schema migration state.

Reaction bytes are not stored in SQLite by default: the journal stores refs and
metadata while `FileReactionStore` stores content-addressed bytes.

Core JournalPort operations commit command/event/state changes atomically. Run
creation, status changes, impulse acceptance, process scheduling and
transitions, homeostat transitions, and projection saves use these transaction
boundaries. Runtime commands and events are protected against direct updates
and deletes; core facts are appended through JournalPort/backend command paths.

Existing databases may physically retain historical `runtime_pools` and
`delegation_policies` tables. Fresh schema initialization does not create or
require those tables or `idx_delegation_policies_pool`; active code ignores
such physical remnants, and generic CLI inspection does not expose them. They
are migration history, not Fala identity or an active fleet API.

Bridge inbox/outbox operations are optional local envelope handoff helpers, not
shared mutable state or a global transaction. Retention, maintenance, reaction
GC, and projection rebuilds are optional ops layers. Maintenance transactions
cover SQLite row changes only; reaction GC scans SQLite references and then
deletes filesystem CAS blobs as a separate operation, not as one cross-store
transaction. Maintenance is not a normal runtime mutation path.

SQLite initializes with WAL mode, foreign keys, and a busy timeout. The sink is
local-first and requires no Redis, Postgres, queue broker, web server, Docker,
or external service.
