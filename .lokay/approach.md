# Approach plan

<!-- lokay-approach source=deterministic repo=mikolaj92/Fala issue=164 -->

Repository: `mikolaj92/Fala`  
Issue: #164 — src/README kłamie że nie ma paczki Python; tree ma python/fala 0.7.26

## Goal

`src/README.md` mówi, że nie ma paczki Python. `python/fala/` istnieje, `__version__ = \"0.7.26\"`.

## Files likely touched

- `src/README.md`
- `pixi.toml`
- `pyproject.toml`

## Test plan

- Run the smallest useful tests for files touched

## Non-goals

- (none stated)

## Notes

- Trust intentional issue; this plan is evidence for later review, not a human gate.
- Coding agent may refine details but should stay on the stated goal and non-goals.
- Collector boundary: if implementation introduces unbounded collection, ship only a bounded collector patch that starts durably in the background after merge. The coding agent and mill must not populate data or wait for collection to finish.
