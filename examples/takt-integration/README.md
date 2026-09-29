# Fala hosts Takt cascade (subprocess)

Wire **Takt 0.2+** as a Fala subprocess effector. Vocabulary lives in
`fala.domain_packs.takt`; the engine is the sibling Takt product.

```text
~/Developer/OSS/Fala
~/Developer/OSS/takt
```

`fala-package.toml` — correlation path with `subprocess` → `takt_step.sh`
and optional interlock homeostat.

```bash
mise exec -- pixi run takt-domain
```

See [`docs/DOMAIN_PACKS.md`](../../docs/DOMAIN_PACKS.md).
