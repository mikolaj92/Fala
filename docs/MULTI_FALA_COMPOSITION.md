# Multi-Fala Composition

> **Boundary (2026-07):** product composition of **separate Falas** is
> **process host + separate journals + optional envelope handoff**.
> Runtime **pools / fleet selection** are optional multi-runtime machinery,
> not Fala identity. Canonical split:
> [`FALA_HOST_AND_COMPOSITION.md`](FALA_HOST_AND_COMPOSITION.md).

Fala composition uses references and bridge delivery, not global transactions.
Each nested correlator should use a **separate Journal** (Unix recursion rule);
see [`UNIX_AND_CYBERNETICS.md`](UNIX_AND_CYBERNETICS.md).

## Default composition (core)

1. Parent runs a **subprocess** effector (or CLI) with its own `--db` / journal.
2. Child never shares the parent journal path.
3. Results return via subprocess contract (`result.json`) and/or **bridge file**
   export/import the parent explicitly performs.

No mutual discovery. No requirement that Falas “know about” each other.

## Historical / optional inventory

Pieces that exist in the tree for advanced ops (not required for one Fala):

- `RuntimeRef` / `RunRef` / `EventRef`: typed ids in envelopes and records.
- `RuntimePool` + policies (`manual` / `first` / `least_busy` / `round_robin`):
  **multi-runtime selector** — optional control plane.
- `DelegationPolicy`: impulse filters and bridge budgets when using pools.
- adapter kind `fala_runtime`: enqueue to a URI or pool (fleet path).
- bridge outbox/inbox rows: durable local delivery records for envelope ops.

Bridge delivery modes:

- local SQLite delivery: `fala bridge deliver --target-db ...`
- file handoff: `fala bridge export --out delivery.json` and
  `fala bridge import --file delivery.json`
