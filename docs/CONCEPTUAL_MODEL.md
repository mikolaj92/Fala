# Conceptual Model

Fala is a local autonomous **Correlator** and cybernetic mediator. It accepts
typed Impulses, conducts them through named relationships, and records the
resulting memory trace. Its runtime is **event-first**: commands and events
make each state transition observable and replayable.

Unix process composition, the JournalPort, and the process host are the
implementation boundary for that autonomous organ—not its identity. SQLite is
the bundled reference sink; memory and JSONL are interchangeable JournalPort
sinks for tests and Unix-style pipes.

For the combined Unix/cybernetic synthesis, see
[`UNIX_AND_CYBERNETICS.md`](UNIX_AND_CYBERNETICS.md). The historical-to-current
lexicon is [`CYBERNETIC_MAPPING.md`](CYBERNETIC_MAPPING.md).

## Core ontology

The core object is an **Impulse**: a typed information impulse moving through a
run-scoped `CorrelationPath` of autonomous `Effector`s. Domain packs map their
own language onto this vocabulary; they do not redefine the core.

The product is the graph (call graph, behavior graph). `CorrelationPath` is
that graph — the authored value — not a recipe. Small Unix effectors exist so
a human can operate on that graph. An effector is a replaceable executor: a
function, a process, or a non-deterministic step that returns a deterministic
result. Payload format (JSON, TOML, anything else) is a twenty-minute stub.
Fala constructs graphs; the rest can be generated. Lokay and Temida are twin
graphs on this same bet.

| Record | Cybernetic role |
| --- | --- |
| **Impulse** | Typed information entering the receptor (signal, payload, or token) |
| **ImpulseType** / **ImpulseRelation** | Type registry and durable lineage between impulses |
| **CorrelationPath** | Topography of named contracts defining conductivity |
| **Effector** | Autonomous node responsible for its own reaction and regulation |
| **Association** | Measurement or reading in the Correlator memory trace |
| **Reaction** | Materialized output of an effector; bytes live outside the journal |
| **Homeostat** | Durable wait or defensive regulation checkpoint |
| **Process** | Concrete activation attempt of an effector for an impulse |
| **Command** | Idempotent write intent submitted to the Correlator |
| **Event** | Ordered, append-only fact produced by a command |
| **Projection** | Rebuildable read model derived from state and events |
| **Run** | Lifecycle boundary for one local autonomous execution |

## Implementation boundary

```text
AutonomousCorrelator → driver (claim → host → complete)
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
              JournalPort         Process host
          (memory|sqlite|jsonl) (subprocess|native_function)
```

- **Cybernetic layer:** Impulses, conduction, effectors, homeostats, and
  associations.
- **Event-first implementation:** JournalPort batches carry commands and
  events; sinks provide durability and replay.
- **Unix layer:** local process host, separate child journals, CLI streams, and
  optional tee sinks.
- **Not identity:** multi-runtime pools and peer discovery are optional
  composition machinery; see
  [`FALA_HOST_AND_COMPOSITION.md`](FALA_HOST_AND_COMPOSITION.md).

Packages are authored as TOML or canonical JSON and loaded by the Mojo package
surfaces (`load_package_toml`, `load_package_json`). Adapters are
`native_function`, `subprocess`, and `manual_homeostat`.

## Relationship to Takt

Fala provides flat, observable conduction through `CorrelationPath`. Takt is a
separate package with its own local `Wave` type and regulator prototype; it is
not part of Fala's core runtime and is not required to use Fala.

An external adapter may connect a Takt-controlled plant to Fala, but that
adapter must map the current runtime contracts explicitly. Fala does not claim
formal equivalence with any single cybernetic theory; it provides a deliberate
working lexicon and an observable runtime for autonomous information
correlation.

## Peer conduction

Conduction is a named contract, not a success-only workflow edge:

1. **One Process per Effector (per run plan):** the durable activation unit for
   a declared node; a new plan may intentionally activate it again.
2. **Terminal readiness:** at the durable runtime layer, a downstream process
   becomes `ready` when every declared upstream is terminal (`succeeded`,
   `failed`, `cancelled`, or `timed_out`). The pure `CorrelationGraph.readiness`
   helper exposes the narrower `completed`/`failed` projection; it does not
   define the durable runtime state machine.
3. **Peer conduction:** terminal payloads (success output or failure/error
   object) travel under `conduction`; the receiving effector owns the decision.
4. **No dead-upstream cancel:** Fala does not silently cancel dependents when
   an upstream fails. Each autonomous organ decides how to react.

Peer conduction replaced success-only dependency propagation because a central
"dead upstream" rule discarded useful error information and made the scheduler
decide on behalf of the receiver. Fala preserves the terminal payload; the
receiving effector interprets it. This is the design rationale behind “mediator,
not a workflow tyrant.”

Transaction and state invariants are specified in
[`RUNTIME_SEMANTICS.md`](RUNTIME_SEMANTICS.md); process claims, leases, and
execution are specified in [`PROCESS_RUNTIME.md`](PROCESS_RUNTIME.md).
