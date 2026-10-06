---
name: building-with-fala
description: Build mixed software from small autonomous effectors.
version: 0.1.0
author: Patryk Mikołajczyk (mikolaj92), Hermes Agent
license: MIT
platforms: [linux, macos]
metadata:
  hermes:
    tags: [fala, cybernetics, unix, agents, composition]
    related_skills: []
---

# Building with Fala

Use Fala to compose software from small Unix-style programs, native functions,
probabilistic tools, explicit regulation points, and other Falas. This skill
governs architecture and authoring; it does not turn Fala core into an LLM
orchestrator or require a nested Fala for every operation.

## When to Use

Use this skill when:

- creating or changing a Fala package or consumer;
- deciding the boundary of an Effector or `CorrelationPath`;
- mixing deterministic code with an LLM, heuristic, or human decision;
- wrapping a CLI as a subprocess effector;
- deciding whether one responsibility needs a child Fala;
- reviewing code for fat effectors, shared journals, or hidden workflows.

Do not use it to redesign Fala as a fleet, peer mesh, shared database, or
in-process multi-agent runtime.

## Prerequisites

Before authoring:

1. Read `docs/CONCEPTUAL_MODEL.md` for canonical terms.
2. Read `docs/ADAPTER_CONTRACTS.md` for the subprocess boundary.
3. Read `docs/FALA_HOST_AND_COMPOSITION.md` before nesting.
4. Inspect the closest package under `examples/`.

Completion criterion: the intended adapter kinds, journal paths, and external
effects are known before editing the package.

## Mazur Model

Use Marian Mazur's model of an autonomous system as a practical design
language, not as a claim that Fala implements every theoretical quantity.

| Mazur-oriented role | Fala design interpretation |
| --- | --- |
| Environment | Operator, parent organ, files, services, models, and processes outside one Fala |
| Receptor | Typed `Impulse`, schemas, capability, and input validation |
| Correlator | `CorrelationPath`, `conduction`, policy, and observable memory trace |
| Effector | One parent-visible responsibility that acts or returns a result |
| Reaction | Materialized result or footprint of an Effector |
| Association | Recorded reading or measurement |
| Homeostat | Durable wait or explicit regulation checkpoint |
| Resources | Time, attempts, CPU/GPU, memory, tokens, and external capacity supplied by the host/product |

A Homeostat holds a regulatory boundary. Do not hide classify → repair → test →
decide inside it. Model those as peer effectors or a child Fala.

Completion criterion: every proposed node has a clear receptor/input contract,
one effector responsibility, and a reaction/output or explicit homeostatic wait.

## Unix Model

Unix supplies the physical composition rules:

- prefer small programs with explicit inputs and outputs;
- reuse an existing command before writing a framework abstraction;
- pass argv to subprocesses; do not rely on an implicit shell;
- use the manifest and `output/result.json` as the subprocess contract;
- treat stdout/stderr as diagnostics, not the durable result;
- give every child Fala its own process, journal, and result handoff;
- never allow a child to open or mutate its parent's journal;
- deduplicate retryable external effects with stable `execution_id`.

```text
Parent Fala (J_p)
  ├─ native function
  ├─ small Unix process
  ├─ LLM/model process
  └─ child Fala (J_c != J_p)
       ├─ small effector
       ├─ small effector
       └─ homeostat
```

Completion criterion: each process boundary has explicit argv, input, output,
error, timeout/retry, and ownership rules.

## Core Rule: Responsibility, Not Size

An Effector is atomic **at the parent-visible responsibility boundary**. It may
be implemented by one function, one command, one probabilistic process, or one
child Fala.

Choose the smallest sufficient implementation:

| Need | Choice |
| --- | --- |
| Small deterministic Mojo transform | `native_function` |
| Existing CLI, another language, isolation, service/model call | `subprocess` |
| Explicit external or human decision | `manual_homeostat` |
| Parent must observe or regulate several stages | Several peer effectors |
| One parent responsibility needs internal topology, memory, or regulation | Child Fala through `subprocess` |

Do not make every Effector a Fala. Do not compress unrelated stages into a fat
agent. Put detail at the level whose homeostasis requires seeing it.

Completion criterion: for each responsibility, a simpler rejected option and
the reason it is insufficient can be stated.

## Procedure

### 1. Define the environment contract

Write the typed incoming `Impulse`, expected final Reaction, relevant external
effects, and failure payloads before drawing the graph.

Completion criterion: input, output, error, and irreversible effects are
explicit.

### 2. List responsibilities without technologies

Describe each responsibility as one verb and object, such as “normalize
request”, “propose patch”, “run tests”, or “publish artifact”. Separate stages
when the parent must observe, retry, branch on, or regulate them independently.

Completion criterion: no responsibility contains an implicit “and then”.

### 3. Select the smallest sufficient implementation

For each responsibility, consider in order:

1. existing small program;
2. `native_function`;
3. one `subprocess`;
4. several peer effectors;
5. `manual_homeostat` where an explicit decision is required;
6. child Fala for autonomous internal topology.

Stop when the contract, isolation, and observability requirements are met.

Completion criterion: every Effector has an adapter and a reason for its
boundary.

### 4. Localize nondeterminism

Treat an LLM or heuristic as an ordinary subprocess Effector with one semantic
responsibility. Surround it with deterministic preparation and verification:

```text
validate → prepare bounded context → model/tool → schema check → tests → act
```

Record non-secret model/tool identity, relevant configuration, and provenance.
Do not let free-form output directly trigger an irreversible effect.

Completion criterion: every probabilistic result crosses a schema check and a
deterministic test or explicit Homeostat before irreversible action.

### 5. Declare the package

Author TOML or strict JSON with:

- typed impulses and reaction kinds;
- capabilities that state accepted and emitted kinds;
- Effectors with named `conduction`;
- output schemas at uncertain boundaries;
- deliberate timeout, `max_attempts`, and retry policy;
- distinct backend/reaction-store paths for independently owned organs.

Completion criterion: all references resolve and terminal success/error payloads
have an intended receiver.

### 6. Nest only when required

When one parent-visible responsibility needs its own topology, create a child
package and executable entry point. Run it as a subprocess with a journal path
different from the parent and return through `result.json` or an explicit bridge.

Completion criterion: parent and child can be stopped, inspected, and replayed
without either opening the other's journal.

### 7. Verify from atoms outward

Test:

1. each deterministic function or program;
2. each probabilistic boundary with deterministic stubs and malformed outputs;
3. conduction of success and error payloads;
4. timeout, retry, lease expiry, and idempotency behavior;
5. child Fala journal isolation and result handoff;
6. terminal replay and observable commands/events.

Use the project's documented Pixi smoke tasks through the available terminal
tool. Do not invent a CLI `run_until_idle` command; it is an embedded/library
API.

Completion criterion: tests demonstrate behavior and journal boundaries, not
merely successful package parsing.

## Mixed-System Patterns

### Deterministic shell

```text
parse → normalize → validate → write
```

Use one program if this is one parent-visible responsibility. Split it when the
parent needs intermediate recovery or policy.

### Probabilistic proposal

```text
collect → LLM proposes → schema validates → tests decide → publish
```

The LLM proposes. Deterministic Effectors or a Homeostat decide whether the
proposal advances.

### Composite responsibility

```text
Parent: [implement feature]
             │
             └─ Child Fala
                  plan → edit → test → regulate
```

The parent sees one responsibility; the child exposes the stages needed for its
own regulation.

## Pitfalls

- **Fat agent:** one subprocess plans, edits, tests, retries, and publishes while
  hiding all intermediate state.
- **Fractal ceremony:** every trivial conversion becomes a child Fala.
- **Wrong level:** internal child details pollute the parent graph even though
  parent policy never uses them.
- **Shared being:** parent and child share SQLite or reaction-store ownership.
- **Fake validation:** an LLM response is accepted because it parses.
- **Retry confusion:** repeated sampling is treated as correctness, or an
  external effect is repeated without stable deduplication.
- **Theory inflation:** token counters, caches, or models are called Homeostats
  without an explicit regulatory checkpoint.
- **Runtime invention:** documentation aspirations are mistaken for implemented
  adapter kinds or CLI commands.

## Verification

Before finishing a Fala design, confirm all items:

- [ ] Every Effector owns one parent-visible responsibility.
- [ ] Each responsibility uses the smallest sufficient implementation.
- [ ] Parent-relevant stages are separate Effectors.
- [ ] Every nondeterministic boundary is named, typed, and validated.
- [ ] Proposal/generation is separate from verification.
- [ ] Irreversible effects have deterministic checks or explicit regulation.
- [ ] Retryable external effects deduplicate by stable `execution_id`.
- [ ] Every child Fala is a subprocess with its own journal.
- [ ] Parent/child handoff is explicit and no journal is shared.
- [ ] Tests execute behavior, failure paths, and replay—not only manifest load.

Read `docs/AGENT_PROGRAMMING_MODEL.md` when the correct composition level is
still ambiguous.
