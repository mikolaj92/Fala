# SQLite Backend

SQLite is the bundled **reference journal sink**. The public sink is
`SqliteJournalPort`; the engine is `NativeJournal`. See
[`JOURNALPORT_CORE_PATH.md`](JOURNALPORT_CORE_PATH.md) and
[`RUNTIME_SEMANTICS.md`](RUNTIME_SEMANTICS.md).

The backend stores runs, impulses, types, relations, associations, reaction
metadata, processes, homeostats, projections, append-only commands and
events, optional bridge inbox/outbox, and a schema version stamp.

Reaction bytes are not stored in SQLite by default. The journal stores refs
and metadata; `FileReactionStore` stores content-addressed bytes.

`NativeJournal` and `NativeDomainStore` helpers commit command/event/state
changes atomically. `SqliteJournalPort.append_batch` is not that guarantee:
it dispatches only the leading unit.

Existing databases may physically retain historical `runtime_pools` and
`delegation_policies` tables. Fresh schema initialization does not create
them. Active code ignores those remnants; they are not an API.

Bridge inbox/outbox is optional local envelope handoff, not a global
transaction. Retention, maintenance, reaction GC, and projection rebuilds
are optional ops. Maintenance covers SQLite row changes only; reaction GC
scans SQLite references and then deletes filesystem CAS blobs as a separate
operation.

SQLite initializes with WAL mode, foreign keys, and a busy timeout. The sink
is local-first and requires no Redis, Postgres, queue broker, web server,
Docker, or external service.
