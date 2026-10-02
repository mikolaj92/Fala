# Fala host and composition

Single engine: Mojo. `python/fala` is a JSON host binding, not another journal,
driver, or runtime.

One `host_run_package` call is one graph. Order, repetition, and accumulation
are conduction edges or effectors. A host loop that calls `host_run_package`
once per item is a second process. The host may write the item list into the
package and call once. It may not drive the items.

Fala has no HTTP application, templates, or frontend assets.

## Process host

The native process host targets Darwin and Linux
(`libfala_process_host.dylib` / `.so`). Windows is out of scope. Linux needs
glibc 2.29+ for `posix_spawn_file_actions_addchdir_np`. Build is wired through
`tools/mojo_sql_run.sh` when host smokes run.

```text
driver → claim → host starts argv (or native_function in-process)
       → child writes output/result.json
       → driver commits complete/fail via journal
```

`native_function` proves the organ without OS spawn. Packages, isolation,
manifests, and redaction assume the subprocess boundary.

## Graph CLI

These commands only read package files. They do not open journals or run
adapters:

```bash
fala graph expand --package package.toml
fala graph validate --package package.toml
fala graph fingerprint --package package.toml
fala graph diff --before old.fala-package.toml --after new.fala-package.toml
```

All four emit stable JSON. Expansion materializes bounded templates.
Fingerprints cover canonical topology, contracts, and policies. Diff
classifies node, edge, condition, terminal, capability, adapter (including
`child_path` and subprocess command/env), output contract, config, retry,
timeout, and runtime-policy changes.

For a durable run:

```bash
fala explain --db state.sqlite --package package.toml --run-id RUN \
  [--process-id ID | --terminal ID]
```

Output is canonical JSON from the package graph and committed journal facts.
Full payloads and adapter environments are omitted.

Whole-graph rehearsal uses the materialized plan and journal while replacing
every adapter with fixture outcomes:

```bash
fala rehearse --package package.toml --fixture acceptance.json --path-id ship \
  --journal rehearsal.sqlite --report rehearsal.json
```

Production argv and native functions are never called. A passing fixture is
not proof that an agent told the truth. See
[output contracts](OUTPUT_CONTRACTS.md).

## Nested work

A nested correlator is a separate process with its own journal:

```text
Parent Fala  (journal J_p)
  └─ subprocess / child_path
        Child  (journal J_c, J_c ≠ J_p)
              └─ result.json and/or explicit bridge envelope
```

`child_path` is authored in the package and compiled to argv by the Python
host (`python/fala/child_path.py`). Native CLI dispatch does not compile it.
Required fields, retention, and the parent `path_result` are in
[`ADAPTER_CONTRACTS.md`](ADAPTER_CONTRACTS.md). Neither parent nor child
writes the other journal.

Optional bridge handoff (operator-chosen paths, not discovery):

```text
fala bridge list --db JOURNAL.sqlite --run-id RUN_ID
fala bridge deliver --db SOURCE.sqlite --run-id RUN_ID --delivery-id ID \
  --target-db TARGET.sqlite --now RFC3339
fala bridge export --db JOURNAL.sqlite --run-id RUN_ID --delivery-id ID --out delivery.json
fala bridge import --db JOURNAL.sqlite --file delivery.json
```

v1 merge validates the envelope and budgets if present. No raw StateFact
injection into parent privileged tables.

## Python host

`host_drive` / `open_memory` drive the memory path. `open_sqlite`,
`host_run_package`, and `delete_terminal_run` cross a JSON boundary into Mojo.
The `host_run_package` binding uses an empty native-function registry unless
another native boundary supplies one.

`fala.sdk` helps a Python subprocess effector read `FALA_EFFECTOR_MANIFEST`
and write `result.json`. It is the language-neutral wire helper, not a
`python_function` adapter.

The official Mojo 1.1 bridge is `PyInit_*` + `PythonModuleBuilder`.
`ensure_native` stays because `import mojo.importer` cannot pass package
import paths.

Core CLI mutations use explicit `--db`, `--run-id`, and `--now` where
required. `run_until_idle` is not a CLI command.
