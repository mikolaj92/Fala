# Fala hosts Takt cascade (subprocess)

Wire **Takt 0.2+** as a Fala subprocess effector. Vocabulary lives in
`fala.domain_packs.takt`; the engine is the sibling Takt product.

## Layout

```text
~/Developer/OSS/Fala
~/Developer/OSS/takt    # exclusive Mojo cascade engine
```

## Package

`fala-package.toml` — correlation path with `subprocess` → `takt_step.sh` and
optional interlock homeostat.

## Manual smoke (requires Takt checkout)

```bash
# From Takt:
# TAKT_REQUEST_PATH=examples/fixtures/cascade_evaluate.request.json ./tools/takt_step.sh

# Vocabulary only (no Takt binary required):
mise exec -- pixi run takt-domain
```

See [`docs/TAKT_DOMAIN_PACK.md`](../../docs/TAKT_DOMAIN_PACK.md).
