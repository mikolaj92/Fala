# Approach plan

<!-- lokay-approach source=deterministic repo=mikolaj92/Fala issue=226 -->

Repository: `mikolaj92/Fala`  
Issue: #226 — Cleanup: docs bez dual-path CPython engine

## Goal

Living docs describe only the Mojo engine + thin python/fala host — no “Python engine path” / active CPython-engine wording.

## Files likely touched

- `docs/ADAPTER_CONTRACTS.md`
- `docs/DOMAIN_PACKS.md`
- `docs/MIGRATIONS.md`

## Test plan

- ADAPTER_CONTRACTS.md / DOMAIN_PACKS.md (and any other living doc, not MIGRATIONS history) no longer imply a second engine
- History may remain only in docs/MIGRATIONS.md / FALA_ARCHITECTURE_STATUS “REMOVED” tables
- No code changes required
- `rg -n 'CPython engine|Python engine path' docs/*.md README.md` → only intentional REMOVED/history hits (or none in living contract docs)

## Non-goals

- Rewriting conceptual cybernetics docs; deleting MIGRATIONS.md.

## Notes

- Trust intentional issue; this plan is evidence for later review, not a human gate.
- Coding agent may refine details but should stay on the stated goal and non-goals.
- Collector boundary: if implementation introduces unbounded collection, ship only a bounded collector patch that starts durably in the background after merge. The coding agent and lokay must not populate data or wait for collection to finish.
