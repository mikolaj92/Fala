# Changelog

Fala follows semantic versioning for the product surface (packages, adapters,
CLI, journal/driver contracts).


## 0.7.9

**First host drive accepts a durably staged correlation plan (#112).**

- `run_correlation_path` now distinguishes a staged, never-driven plan with no
  adapter bindings from a damaged replay and atomically persists its complete
  binding set before execution.
- Partial binding sets and missing bindings after execution evidence remain
  fail-closed; complete persisted bindings stay authoritative across restart.


## 0.7.7

**Safe terminal-run deletion for Python hosts (#108).**

- New public API: `fala.delete_terminal_run(db_path, run_id) -> dict`.
- Native Mojo entrypoint `delete_terminal_run` validates terminal status
  (`completed`, `failed`, `cancelled`, `timed_out`) after `BEGIN IMMEDIATE`
  and before suspending append-only triggers; unknown / blank / active runs
  fail closed with triggers restored.
- Reuses the existing atomic run-scoped deletion transaction (no direct SQLite
  deletes from Python).
- `_native.delete_terminal_run_json` bridge + focused Python binding tests.

## 0.7.6

**Subprocess `inherit_env` receives host process environment via Python host (#108).**

- `host_run_package` ships `host_environment` (`dict(os.environ)`) in the Mojo
  request JSON.
- Before dispatch, `host_run_package_json` calls
  `materialize_host_environment_into_adapter`: base keys (`PATH`, `HOME`,
  `TMPDIR`, `LANG`, `LC_ALL`, `TZ`), each `inherit_env` key, and
  `${env:NAME}` interpolations are baked into `adapter.env` as literals and
  `inherit_env` is cleared — so the durable driver no longer needs a live host
  map at `execute_subprocess` time.
- Fail-closed when an `inherit_env` key is absent from the host process:
  `host environment missing for inherit_env key: …`.
- Python binding regression: `test_host_run_package_inherit_env_from_host_process`.
- Also exports `fala_host_getenv` on the process-host C ABI (for future pure-Mojo
  callers); the Python durable path does not depend on it.

## 0.7.5

**Python durable host: sqlite.fire auto-build on first use (#106).**

- `open_sqlite` / `host_run_package` call `ensure_sqlite_fire_library()` before the Mojo host:
  if `libsqlite_fire.{dylib,so}` is missing under hatch-shipped `vendor/sqlite.fire/native`,
  run `make -C native` once (fail-closed with toolchain / libsqlite3 diagnosis).
- Memory path (`host_drive` / `open_memory`) still does **not** require sqlite.fire.
- Escape hatch: `FALA_SKIP_NATIVE_BUILD=1` skips the build attempt (durable APIs fail closed).
- Docs: SQLite remains an optional journal sink, not product identity.

## 0.7.4

- host_run_package: encode inputs via Mojo to_string (fix double-encoding / invalid process error JSON)

## 0.7.3

- host_run_package: always JSON-encode effector_inputs/inputs field values

## 0.7.2

- host_run_package: load .json packages via load_package_json

## 0.7.1

- `host_run_package`: `effector_inputs`, `effector_configs`, `command_overrides`

## 0.7.0

**Python host: `host_run_package`** — durable SQLite + TOML package + path drive.

### Added
- `fala.host_run_package(db_path, package_path, path_id, …)` via Mojo `run_correlation_path`
- Supports package adapters: `subprocess`, `native_function`, `manual_homeostat`

## 0.6.0

**Python host: effector SDK + SQLite journal probe (still Mojo-only engine).**

### Added
- `fala.sdk` — pure-Python `FALA_EFFECTOR_*` helpers (`run_manifest_effector`, `output`, …)
  for subprocess organs; no CPython runtime engine
- `fala.open_sqlite(path)` — open/create durable SQLite journal via Mojo `NativeJournal`
- Build path includes `vendor/sqlite.fire` for native journal

### Still not present
- CPython `RuntimeBackendService` / full 0.2.1 API (intentionally removed from product)
- Full `run_correlation_path` Python host (next: package drive with subprocess effectors)

## 0.5.0

**Thin optional Python host binding** (memory path only).

### Added
- `python/fala/`: `host_drive`, `host_drive_json`, `open_memory` / `MemoryHost`
- JIT `_native` extension over Mojo memory runtime (create_run → impulse → path → drive)
- Python smoke tests

### Unchanged
- Exclusive Mojo product core; CLI / SQLite multi-organ / ops packs stay outside the binding
- No dual engine; subprocess + CLI remain primary for full host composition

## 0.4.0

**Thin core, domain packs, and host/composition completeness** on the exclusive
Mojo product line.

### Core / architecture

- Ops bodies extracted from `NativeDomainStore` into `ops_maintenance`,
  `ops_projections`, and `ops_bridge` (real free-function implementations).
- Essential vs optional layers documented (`FALA_ARCHITECTURE_STATUS`,
  `JOURNALPORT_CORE_PATH`).
- CLI progressive disclosure (`# core` / `# ops`) and `native_cli_help` split.
- Process host **Darwin + Linux** (`.dylib` / `.so`, `/proc/self/exe`).
- Multi-claim composition: `drive_ready_batch` / `claims_per_round`; multi-workspace
  composition documented and smoked.
- `rearm_homeostat` (#68): atomic re-open on a waiting run with attempt accounting.

### Domain packs

- **Signals** — essential-variable helpers (`signals.reading`, threshold homeostat).
- **Takt** — cascade vocabulary for sibling **takt 0.2+** (`takt.cascade_request`,
  plant layer / error signal / safety interlock / projection).
- **Splot** pack remains; multi-organ examples (`examples/multi-organ/`,
  `examples/takt-integration/`).

### Docs

- README leads with composer-of-single-purpose-processes mental model (#34).
- Process runtime docs describe multi-claim and multi-workspace composition.

### Proof

```bash
mise exec -- pixi run full-smoke
mise exec -- pixi run extended-smoke
# packs:
mise exec -- pixi run splot-domain
mise exec -- pixi run signals-domain
mise exec -- pixi run takt-domain
mise exec -- pixi run multi-organ-example
```

## 0.3.0

**First exclusive Mojo product release.**

### Product

- Runtime is **Mojo only** under `mojo/fala/` (organ, journal, driver, process
  host, package loader, CLI).
- Packages are **TOML** (or canonical JSON). YAML packages are unsupported.
- Effector adapters: `subprocess`, `native_function`, `manual_homeostat`.
- Journal port with InMemory, SQLite (reference sink), JSONL, Tee.
- Native process host for OS children (core, not optional peel).
- Domain pack **Splot** (vocabulary) + integration smoke hosting sibling
  **Splot 0.3.1+** as a subprocess effector.

### Removed

- CPython product engine (`src/fala`, Python runtime demos).
- `python_function` adapter (unknown kind).
- Fleet / multi-runtime / `fala_runtime` peer mesh.
- Optional Python/YAML example leftovers.

### Proof

```bash
mise exec -- pixi run full-smoke
mise exec -- pixi run extended-smoke
# optional sibling Splot:
mise exec -- pixi run splot-integration
```

### Notes

- Historical design notes may still mention the Python-era path; they are not
  the product contract. Prefer `docs/FALA_ARCHITECTURE_STATUS.md` and
  `docs/ADAPTER_CONTRACTS.md`.

## 0.2.2

Last line that still mixed Mojo port work with a CPython-era tree. Superseded
for product purposes by **0.3.0 Mojo exclusive**.
