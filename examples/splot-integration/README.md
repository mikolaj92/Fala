# Fala × Splot (Mojo)

Fala hosts the **Splot 0.3+** arbitration engine as a **subprocess**
effector. Fala does not import Splot.

Sibling repos (default):

```text
~/Developer/OSS/Fala
~/Developer/OSS/Splot   # v0.3.0+
```

Override with `SPLOT_ROOT=/path/to/Splot`.

`fala-package.toml` points `adapter.kind = subprocess` at
`Splot/tools/splot_step.sh`. `request.json` is a sample arbitration payload.

| Env | Meaning |
| --- | --- |
| `FALA_EFFECTOR_INPUT_DIR` | work/input |
| `FALA_EFFECTOR_OUTPUT_DIR` | work/output |
| `FALA_EFFECTOR_MANIFEST` | work/input/manifest.json |

Splot writes `output/result.json`. For Mojo under a sanitized host env, pass
`PATH`, `CONDA_PREFIX`, `MODULAR_HOME` (or rely on `splot_step.sh` deriving
them from `FALA_PIXI_ENV`).

```bash
mise exec -- pixi run splot-integration
```

The smoke checks that `selected_candidate_id` is the best live camera
(`cam_a`).
