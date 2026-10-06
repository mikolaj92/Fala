# Agent programming model

Fala is a framework that humans and coding agents use to build mixed software:
a deterministic, observable runtime composes small Unix-style programs,
probabilistic tools, manual regulation points, and other Falas. The agent is the
program author. Fala is the execution and memory organ; it does not secretly
turn every process into an agent or rewrite the topology at runtime.

## The governing rule

An **Effector is one responsibility at the boundary where its parent observes
it**. Its internal implementation may be:

- one native Mojo function;
- one existing Unix/CLI program;
- one isolated subprocess written in any language;
- one probabilistic or LLM-backed subprocess;
- one child Fala composed of smaller effectors.

Atomicity refers to responsibility, not line count or internal process count.
Use the smallest implementation that preserves a clear contract and sufficient
observability.

```text
                    one parent-visible responsibility
                                  │
                       ┌──────────┴──────────┐
                       │                     │
                 atomic program        child Fala
                                           │
                                small internal effectors
```

Do not introduce a child Fala when a function or existing command is enough.
Do not hide several parent-relevant stages in one fat subprocess merely to make
the graph look small.

## Mazur as the design model

Fala deliberately uses the autonomous-system vocabulary associated with Marian
Mazur and the Polish cybernetic tradition. This is a working engineering
lexicon, not a claim of complete formal equivalence.

| Cybernetic element | Fala authoring interpretation |
| --- | --- |
| **Environment** | Operator, parent Fala, filesystem, services, models, and programs outside one organ |
| **Receptor** | Typed `Impulse`, input schema, capability contract, and adapter validation boundary |
| **Correlator** | `CorrelationPath`, named `conduction`, memory trace, and the policy connecting information to action |
| **Effector** | One autonomous, parent-visible responsibility that produces a result or environmental effect |
| **Reaction** | Materialized output or footprint of an effector; bytes live outside the journal |
| **Association** | Reading or measurement recorded in correlator memory |
| **Homeostat** | Durable wait or regulation checkpoint that keeps a decision explicit |
| **Energy/resources** | Time, attempts, CPU/GPU, memory, token/API budgets, and external capacity supplied by the host/product |

Not every theoretical quantity is a runtime type. An agent must still reason
about resource limits and regulation even when the product layer, rather than
Fala core, supplies those limits.

A homeostat detects or holds a regulatory boundary. It must not conceal a fat
repair workflow. If recovery needs classify → repair → test → decide, author
those as separate effectors or as a child Fala.

## Unix supplies the composition mechanism

Cybernetics defines the organ; Unix defines how organs are connected:

- prefer small programs with explicit inputs and outputs;
- invoke children with argv, never an implicit shell pipeline hidden in core;
- cross the subprocess boundary through the Fala manifest and `result.json`;
- treat stdout/stderr as observable diagnostics, not the durable result contract;
- give each nested Fala its own process and journal;
- hand results back explicitly; never let a child open the parent's journal;
- use stable `execution_id` to deduplicate external effects across retries.

```text
Parent Fala (journal J_p)
  └─ subprocess effector
       └─ Child Fala (journal J_c, J_c != J_p)
            ├─ native function
            ├─ Unix program
            ├─ probabilistic subprocess
            └─ homeostat
                 │
                 └─ result.json / explicit bridge → parent
```

A child Fala is optional. Recursion is available when the contract needs an
internal topology, memory, isolation, or regulation; it is not mandatory
ceremony.

## Choosing an implementation

Apply this decision order to every responsibility:

1. **Reuse a small existing program** if its contract already fits.
2. **Use `native_function`** for a small deterministic Mojo transformation.
3. **Use `subprocess`** for a CLI, another language, OS isolation, an external
   service client, or an LLM/model invocation.
4. **Use several peer effectors** when their intermediate results, failures, or
   policies matter to the parent organ.
5. **Use `manual_homeostat`** when progress requires an explicit external or
   human decision.
6. **Use a child Fala** when one parent-visible responsibility needs its own
   multi-step topology, journal, lifecycle, or homeostatic loop.

Stop at the first choice that is sufficient. Complexity alone is not a reason
to erase boundaries, and conceptual elegance is not a reason to add processes.

## Mixing determinism and nondeterminism

Fala does not try to remove nondeterminism. It localizes it inside explicit
contracts and surrounds it with deterministic preparation, validation, and
regulation.

```text
Impulse
  → validate                    deterministic
  → collect bounded context     deterministic/environmental
  → model or heuristic          probabilistic
  → validate output schema      deterministic
  → compile/test/check policy   deterministic
  → act or open homeostat       explicit effect/regulation
```

For every nondeterministic effector:

1. Give it one semantic responsibility.
2. Use structured input and output contracts.
3. Record model/tool identity, relevant configuration, and provenance in
   appropriate metadata; do not put secrets there.
4. Validate output before conduction.
5. Separate proposal/generation from verification.
6. Prefer property and contract tests over exact prose matching.
7. Decide retry policy deliberately. Retry is at-least-once execution and may
   repeat an external effect; it is not proof that a new sample is better.
8. Put irreversible actions after deterministic checks or an explicit
   homeostat, and deduplicate them with stable `execution_id`.

An LLM needs no privileged adapter kind. It is normally a subprocess effector
that obeys the same language-neutral contract as every other program.

## Authoring a Fala package

Use this sequence:

1. State the environment-facing input and desired reaction.
2. Declare typed `impulse_types`, `reaction_kinds`, and any associations needed
   for observable measurements.
3. List responsibilities without choosing technologies yet.
4. Merge responsibilities only when the parent never needs to observe or
   regulate them separately.
5. Select the smallest sufficient implementation using the decision order
   above.
6. Declare capabilities and a `CorrelationPath` with named `conduction`.
7. Define output schemas and retry/timeout behavior at each uncertain boundary.
8. If nesting, define a separate child package, process entry point, journal,
   and explicit result handoff.
9. Test each effector contract, then the path, then crash/retry behavior around
   external effects.
10. Inspect commands, events, processes, reactions, and journals to verify that
    the intended boundaries are visible.

A design is complete when every effector has one responsibility, every
nondeterministic boundary is named and validated, every external effect has an
idempotency policy, and every nested Fala owns a distinct journal.

## Patterns

### Small deterministic path

```text
parse → normalize → emit
```

Use separate effectors only if those stages need separate observation,
recovery, or reuse. Otherwise one small program can legitimately implement the
single parent-visible responsibility.

### Mixed semantic path

```text
collect context → LLM proposes → schema validator → tests → publish
```

The model proposes; deterministic effectors establish whether the proposal may
advance.

### Composite effector

```text
Parent: [implement feature]
             │
             └─ Child Fala
                  plan → edit → test → regulate
```

The parent sees one responsibility. The child exposes the stages that its own
regulation needs.

## Anti-patterns

- A single agent subprocess that plans, edits, tests, retries, publishes, and
  hides all intermediate state from the parent.
- A child Fala for every trivial transform.
- A flat parent graph that exposes internal details irrelevant to parent
  regulation.
- Parent and child sharing a SQLite file or process ownership.
- Treating an LLM response as valid because it is parseable.
- Retrying a non-idempotent external action without durable deduplication.
- Calling a cache, token counter, or model a Fala homeostat without an explicit
  regulatory checkpoint.

## Related contracts

- [`CONCEPTUAL_MODEL.md`](CONCEPTUAL_MODEL.md) — canonical ontology.
- [`UNIX_AND_CYBERNETICS.md`](UNIX_AND_CYBERNETICS.md) — synthesis of identity
  and process composition.
- [`FALA_HOST_AND_COMPOSITION.md`](FALA_HOST_AND_COMPOSITION.md) — parent/child
  process and journal boundary.
- [`ADAPTER_CONTRACTS.md`](ADAPTER_CONTRACTS.md) — subprocess wire contract.
- [`PROCESS_RUNTIME.md`](PROCESS_RUNTIME.md) — retries, leases, and execution.
- [`../skills/building-with-fala/SKILL.md`](../skills/building-with-fala/SKILL.md)
  — concise procedure loaded by coding agents.
