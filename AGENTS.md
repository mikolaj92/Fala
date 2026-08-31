# AGENTS

Fala exists to compose **small Unix-style process steps**. It does not hide fat
multi-stage work inside one effector.

Fala's engine is Mojo; Python is a thin host. New work that does not need
Word, LLM, or OOXML gymnastics is written in Mojo. The official Mojo 1.0
bridge is `PyInit_*` + `PythonModuleBuilder` in `python/fala/_native.mojo`.
The official `import mojo.importer` hook cannot pass package import paths
(`EmberJson`, `sqlite.fire`) — that is a 1.0 limitation, so `ensure_native`
stays: same `PyInit`, same `__mojocache__`, explicit `-I` on
`mojo build --emit shared-lib`. Do not replace it with the importer until
Modular adds import paths. Do not grow a second Python orchestrator. Model
kernels (dflash / M5 Ultra) do not belong in Fala.

The product is the graph (call graph, behavior graph). Small Unix effectors
exist so a human can operate on that graph. An effector is an executor: a
function, a process, or a non-deterministic step that returns a deterministic
result. Payload format (JSON, TOML, anything else) is a twenty-minute stub.
Fala constructs graphs; the rest can be generated. Lokay and Temida are twin
graphs on this same bet.

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

## Local proof

The mill runs `[tool.lokay] test` → `mise exec -- pixi run full-smoke`. That is the documented Mojo gate in README/`pixi.toml`. Do not invent `uv run --extra dev pytest` as the Fala verifier.
