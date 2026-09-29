# Fala Architecture Status

**Product: 0.9.4** · Mojo engine + optional thin Python host binding.

Fala composes small programs into a graph. A node is one effector with a
named `output_schema`. An LLM can be a node; it is not the runtime.

Happy path: **package → impulse → parent `run_until_idle` → configured
persistence**. `run_until_idle` is an embedded/library API, not a CLI command.

| Piece | Role | Modules |
| --- | --- | --- |
| Graph / organ | Impulse ontology, correlation advance, process state | `correlation*`, `processes`, `status`, `models*` |
| JournalPort | Generic `append_batch` / `claim_next` / load | `journal_port`, `memory_journal`, `sqlite_journal_port`, `jsonl_journal`, `tee_journal` |
| SQLite core | Command/event/state and lease transactions | `journal` (`NativeJournal`), `domain_store` (`NativeDomainStore`) |
| Driver + host | claim → adapter → complete/fail/wait | `native_driver`, `native_process_host`, `adapters` |
| Adapters | `subprocess`, `native_function`, `manual_homeostat`, `child_path` | `adapters`, `validation` |
| Package / CLI | TOML/JSON package, one-journal inspect | `native_package`, `package`, `native_cli_surface` |

`NativeDomainStore` is not a `JournalPort` implementation. SQLite authority is
the direct helpers on `NativeJournal` and `NativeDomainStore`. Other sinks do
not inherit SQLite atomicity. See
[`JOURNALPORT_CORE_PATH.md`](JOURNALPORT_CORE_PATH.md).

## Optional ops

Not required to compose a small flow: `ops_maintenance`, `ops_bridge`,
`ops_projections`, and the CLI ops verbs (`maintain-journal`, `gc`,
`projections rebuild`, `bridge *`). Essential paths must not import `ops_*`.

## Tree

| Path | Role |
| --- | --- |
| `mojo/fala/` | Engine |
| `python/fala/` | Thin JSON host binding and subprocess SDK |
| `mojo/smoke/` + `pixi.toml` | Proof (`full-smoke`, `extended-smoke`) |
| `examples/` | Packages and domain vocabulary |
| `vendor/` | Gitignored EmberJson and sqlite.fire checkouts |

No web application. No second engine. Release chronology is
[`CHANGELOG.md`](../CHANGELOG.md).

```bash
mise exec -- pixi run full-smoke
```
