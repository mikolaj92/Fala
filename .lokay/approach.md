# Approach plan

<!-- lokay-approach source=deterministic repo=mikolaj92/Fala issue=186 -->

Repository: `mikolaj92/Fala`  
Issue: #186 — host_run_package rejects active process placeholder as invalid error JSON

## Goal

`fala.host_run_package` in Fala `0.7.28` can raise `sqlite.fire: code=1: journal: invalid process error JSON` while a real subprocess effector is still active, even though the persisted `error_json` is valid JSON text `{}`. The host exception strands the active process row and prevents the consumer from safely continuing or reporting the run.

## Files likely touched

- `mojo/fala/native_driver.mojo` — EmberJson-quote adapter error JSON so C0 controls stay parseable
- `mojo/fala/journal.mojo` — same quoting for journal-built JSON strings
- `python/fala/_native.mojo` — decode `{}` placeholders; fail-closed stored JSON names run/process/field
- `python/fala/host.py` — document the non-terminal `{}` contract
- `python/tests/test_python_binding.py` — active-placeholder, control-stderr, and fail-closed regressions
- `python/tests/conftest.py` — pin `FALA_HOME` to this checkout and compile the argv subprocess fixture before collection
- `python/tests/fixtures/subprocess_one.fala-package.toml` — control package from the issue (unchanged)

## Test plan

- A regression test reproduces the active-process state that currently throws.
- `{}` for a non-terminal process is handled according to a documented typed contract.
- No host exception or forced consumer-level failed run occurs for a valid active snapshot.
- Re-drive reaches and returns the eventual terminal process result.
- Existing fail-closed coverage for genuinely malformed stored result JSON remains green.

## Non-goals

- (none stated)

## Notes

- Trust intentional issue; this plan is evidence for later review, not a human gate.
- Coding agent may refine details but should stay on the stated goal and non-goals.
- Collector boundary: if implementation introduces unbounded collection, ship only a bounded collector patch that starts durably in the background after merge. The coding agent and mill must not populate data or wait for collection to finish.
