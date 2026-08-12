# AGENTS

Fala exists to compose **small Unix-style process steps**. It does not hide fat
multi-stage work inside one effector.

## Non-negotiable

1. **Single-purpose effectors.** One effector does one job. Multi-step work is
   multiple effectors and/or correlation paths — not one fat effector.
2. **Plural journals are normal.** Consumers may each keep their own Fala
   journal. Separate journals across consumers are expected and OK.
3. **Nested Fala is valid.** A consumer host may run an inner Fala package
   (subprocess + separate child journal). Parent and child stay separate
   beings with an explicit handoff.

## Do / don't

| Do | Don't |
| --- | --- |
| Split pipelines across effectors / paths | Cram a multi-stage workflow into one effector |
| Give each organ its own journal | Share a parent journal with a nested child |
| Nest via `subprocess` + child `--db` | Treat nested work as in-process multi-stage logic |

## Pointers

- [`docs/FALA_HOST_AND_COMPOSITION.md`](docs/FALA_HOST_AND_COMPOSITION.md) — host boundary and nested composition
- [`docs/UNIX_AND_CYBERNETICS.md`](docs/UNIX_AND_CYBERNETICS.md) — Unix composition model
- [`docs/CONCEPTUAL_MODEL.md`](docs/CONCEPTUAL_MODEL.md) — ontology and conduction
