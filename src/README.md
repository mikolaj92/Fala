# `src/` is not a product package

Fala’s product engine is **Mojo only** (`../mojo/fala`).

The former CPython engine (`src/fala`) was removed. The optional thin Python
host binding lives at `../python/fala/` (package `fala` 0.7.30) and tracks the
product version. It is a JSON bridge to Mojo, not a second engine.

Examples live under `../examples/` as native TOML packages.

`check_stamps.py` fails closed if `src/README.md` stops naming that binding or
if product version banners drift away from `pyproject.toml`.
