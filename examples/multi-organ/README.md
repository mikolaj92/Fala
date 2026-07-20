# Multi-organ composition example

Compose **Fala** as a thin host with two domain organs:

1. **Signals** vocabulary (`signals.reading`) — essential-variable style regulation
2. **Splot** vocabulary (`splot.arbitration_case`) — arbitration organ as subprocess

## Mental model (composer)

```text
operator / environment
        │
        ▼
   Fala (journal J_p)
        │
        ├── effector: native_function  (signals map / EV check)
        └── effector: subprocess       (Splot or any argv organ)
                │
                ▼
           child process + optional child journal J_c ≠ J_p
                │
                └── result.json → parent reaction / conduction
```

- One Fala = one organ + one journal + one claim loop (or multi-claim batch).
- Nested work = **subprocess + separate journal path**, not a shared DB.
- Domain packs only name impulses; business logic stays in organs (Splot, signal tools, …).

## Files

| Path | Role |
| --- | --- |
| `fala-package.toml` | Correlation path: `ingest_signal` → `threshold_gate` → `arbitrate` |
| `request.json` | Sample signal reading impulse payload |

## Run (concept)

```bash
# From repo root, with host built:
mise exec -- pixi run host-smoke   # proves subprocess host
# Package is TOML-loadable via Fala package APIs; full end-to-end with a
# live Splot binary depends on a Splot install on PATH (see splot-integration).
```

See also:

- `examples/splot-integration/` — real Splot subprocess handoff
- `docs/DOMAIN_PACKS.md` — Splot + Signals vocabulary
- `docs/PROCESS_RUNTIME.md` — multi-claim / multi-workspace composition
