# Changelog

Fala follows semantic versioning for the product surface (packages, adapters,
CLI, journal/driver contracts).

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
