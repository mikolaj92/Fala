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

## Load / proof

Package validates with the same schema as other Fala packages (`adapter = { kind = ... }`,
`conduction` on effectors):

```bash
# From repo root:
mise exec -- pixi run multi-organ-example
# asserts load_package_toml + three effectors (native_function, manual_homeostat, subprocess)
# and that the subprocess command uses child.sqlite (separate journal).
```

Full end-to-end with a live Splot binary still depends on a Splot install on PATH
(see `examples/splot-integration/`).

See also:

- `examples/splot-integration/` — real Splot subprocess handoff
- `docs/DOMAIN_PACKS.md` — Splot + Signals vocabulary
- `docs/PROCESS_RUNTIME.md` — multi-claim / multi-workspace composition
