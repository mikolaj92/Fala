# `src/` is not a product package

Fala’s product engine is **Mojo only** (`../mojo/fala`).

The former CPython engine (`src/fala`) was removed. The optional thin Python
host binding lives at `../python/fala/` (package `fala` 0.7.26) and tracks the
product version. It is a JSON bridge to Mojo, not a second engine.

Examples live under `../examples/` as native TOML packages.
