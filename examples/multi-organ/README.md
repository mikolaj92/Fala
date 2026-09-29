# Multi-organ composition example

Compose Fala with two domain organs: Signals vocabulary (`signals.reading`)
and Splot vocabulary (`splot.arbitration_case`) hosted as a subprocess.

| Path | Role |
| --- | --- |
| `fala-package.toml` | Correlation path: `ingest_signal` → `threshold_gate` → `arbitrate` |
| `request.json` | Sample signal reading impulse payload |

```bash
mise exec -- pixi run multi-organ-example
```

Asserts `load_package_toml` plus three effectors (`native_function`,
`manual_homeostat`, `subprocess`) and that the subprocess command uses
`child.sqlite` (separate journal). Live Splot is optional; see
`examples/splot-integration/`.

See [`docs/DOMAIN_PACKS.md`](../../docs/DOMAIN_PACKS.md) and
[`docs/PROCESS_RUNTIME.md`](../../docs/PROCESS_RUNTIME.md).
