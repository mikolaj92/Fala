# JournalPort core path

The generic JournalPort types describe batch, claim, and load operations.
They do not make every sink equivalent.

SQLite authority is the direct transactional helpers on `NativeJournal` and
`NativeDomainStore`. `NativeDomainStore` is not a `JournalPort`
implementation. `SqliteJournalPort.append_batch` dispatches only the leading
unit to `NativeJournal`; later units are ignored as write inputs. Memory,
JSONL, and Tee have weaker persistence: JSONL claim transitions stay in an
in-memory index, and Tee mirrors appends after its primary accepts them.

## Write map

| Flow | Primary API | Boundary |
| --- | --- | --- |
| Accept impulse | `NativeDomainStore.accept_impulse` | Impulse, command, and event in one SQLite transaction. Memory uses `InMemoryJournal.append_batch`. |
| Claim / complete / fail / retry / wait | `NativeJournal` helpers in SQLite; JournalPort claim/batch APIs elsewhere | SQLite claim is `NativeJournal.claim_next_ready`. JSONL claims are not persisted to the file. |
| Homeostat open / terminal | `save_homeostat`, `transition_homeostat` | SQLite command + event + state together. |
| Association / reaction / relation | `record_*` / `put_*` | NativeDomainStore domain transactions; reaction **bytes** live in the reaction store. |

Low-level `put_*` methods are direct SQLite writes. They do not mean every
mutation routes through JournalPort.

## Ops (not the happy path)

Not required for `package → impulse → run_until_idle → configured persistence`:

| API | Module |
| --- | --- |
| `delete_run`, `run_retention`, `maintain_journal`, reaction GC | `ops_maintenance` |
| Bridge outbox/inbox | `ops_bridge`, `bridge_transport` |
| `rebuild_projection(s)` | `ops_projections` |
| Lightweight `put_projection` / get/list | `domain_store` |

A child Fala uses a **separate journal path**. The parent never writes the
child JournalPort.

See [`RUNTIME_SEMANTICS.md`](RUNTIME_SEMANTICS.md) for transaction
invariants.
