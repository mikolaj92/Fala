# Fala × Splot (Mojo)

Fala hosts the **Splot 0.3+** arbitration engine as a **subprocess** effector.
Both products are exclusive Mojo. There is no Python Splot path.

## Layout

- Sibling repos (default):

  ```text
  ~/Developer/OSS/Fala
  ~/Developer/OSS/Splot   # v0.3.0+
  ```

- Override Splot root: `SPLOT_ROOT=/path/to/Splot`

## Package

`fala-package.toml` — correlation path with `adapter.kind = subprocess` pointing
at `Splot/tools/splot_step.sh`.

`request.json` — sample arbitration payload (profile + candidates). Profile paths
may be relative to the Splot repo root (the step script `cd`s there).

## Effector contract

Fala process host provides:

| Env | Meaning |
| --- | --- |
| `FALA_EFFECTOR_INPUT_DIR` | work/input |
| `FALA_EFFECTOR_OUTPUT_DIR` | work/output |
| `FALA_EFFECTOR_MANIFEST` | work/input/manifest.json (includes `input`) |

Splot writes `output/result.json` (decision object). For Mojo under a sanitized
host env, the smoke (and package) should pass `PATH`, `CONDA_PREFIX`,
`MODULAR_HOME` (or rely on `splot_step.sh` deriving them from `FALA_PIXI_ENV`).

## Proof

```bash
# From Fala (requires sibling Splot or SPLOT_ROOT):
mise exec -- pixi run splot-integration
```

The smoke runs Fala’s process host against Splot’s step entry and checks that
`selected_candidate_id` is the best live camera (`cam_a`).
