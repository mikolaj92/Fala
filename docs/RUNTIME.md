# Fala Impulse Runtime

Fala starts from `Impulse`.

The current Impulse-first path lives in `fala.runtime_backend`:

- `AutonomousCorrelator` in `fala.runtime` is the embedded core facade. It uses
  `RuntimeBackendService` and does not import web, API, CLI, or HTTP-client
  modules.
- `Run` records Impulse-first run metadata, lifecycle status, package/correlation-path
  identity, digests, and timestamps.
- `Impulse` is the typed information unit moved by a run.
- `RuntimeCommand` is the idempotent write path.
- `RuntimeEvent` records ordered, command-linked runtime facts.
- `Correlator` is the bundled local backend.
- `RuntimeBackendService` is the service facade for new Impulse-first writes.
- `ImpulseType` records the run-local typed impulse definitions available to a
  correlation path.
- `ImpulseRelation` records durable lineage or dependency edges between
  impulses.
- `Reaction` records immutable reaction metadata in SQLite. Reaction bytes stay
  in a `ReactionStore`, usually the filesystem store.
- `Process` records schedulable Impulse-first work with transactional SQLite
  claim/lease, retry, completion, and failure operations.
- `fala.sdk` provides the effector payload helpers for adapters (`load_manifest`, `conduction`, `find_reaction`, `write_result`, ...).
- `RuntimeRef`, `RunRef`, and `EventRef` identify other Fala runtimes, runs,
  and events without adding a non-SQLite first-party backend.
- `BridgeDelivery` records local SQLite inbox/outbox exchange. Bridge enqueue,
  import, and delivery go through idempotent `RuntimeCommand`s and emit linked
  `RuntimeEvent`s.
- `RuntimePool`, `DelegationPolicy`, and `RuntimeBudget` describe Impulse-first
  delegation targets and budgets for runtime hops, spawned runs, impulses, wall
  time, attempts, and reaction bytes. SQLite stores runtime pools and
  delegation policies, and `fala runtimes list/inspect` exposes them without a
  web server.

New Fala runtime work should use `fala.runtime_backend` or
`fala.runtime`. Impulse APIs use `impulse_id` and `impulse_type`.
Web/API/client exports are outer surfaces and are loaded lazily from `fala`.

Splot arbitration cases and reviews are modeled in `fala.domain_packs.splot`; see

## Core Concepts

- Impulse: the typed unit of information moved by the runtime. It can represent
  a case, reading, event, source, result, or any other domain value.
- Run: the lifecycle record for a local Impulse-first execution. Current
  statuses are `created`, `active`, `waiting`, `completed`, `failed`,
  `cancel_requested`, `cancelled`, and `timed_out`.
- ImpulseType: the registered type metadata for an impulse in a run, including
  media types and value schema metadata.
- ImpulseRelation: a durable relationship between two impulses, used for
  lineage, derivation, dependency, and future wait-graph work.
- RuntimeBackend: the persistence boundary for runs, impulses, impulse types,
  impulse relations, commands, events, associations, reactions, homeostats,
  projections, and bridge inbox/outbox records.
- RuntimeCommand: the only write path for state-changing runtime actions.
  Commands carry an idempotency key, actor, correlation id, causation id, and
  payload.
- RuntimeEvent: ordered facts linked to commands. Events are the audit trail for
  mutations and the source for projections.
- Association: a typed measurement or fact reported about an impulse.
- Reaction: metadata for materialized output such as reports, extracts, or
  evidence snapshots. SQLite stores metadata; content lives in a reaction store.
  `AutonomousCorrelator.record_file_reaction(...)` writes a local file through the
  filesystem reaction store and records only the resulting URI, hash, size, and
  metadata in SQLite.
- Process: a scheduled execution unit. Current statuses are `pending`, `ready`,
  `running`, `waiting`, `retry_wait`, `succeeded`, `failed`,
  `cancel_requested`, `cancelled`, and `timed_out`.
- WaitGraphDiagnostic: a computed local SQLite wait report from process
  `input`/`metadata` wait refs and homeostats.
- Homeostat: a first-class decision point such as human review, approval, expiry, or
  cancellation.
- Projection: a rebuildable read model keyed by run and projection name.
- Lineage: represented through impulse ids, event refs, bridge refs, and domain
  pack metadata rather than document-specific core fields.
- Audit: represented by command actor/correlation/causation metadata plus the
  ordered event log.
- ReactionStore: the content store for reaction bytes. `FileReactionStore` is
  the local content-addressed default; SQLite keeps references and metadata.

## SQLite-Only Core

Fala core ships the SQLite runtime backend. Non-SQLite storage or transport
backends are external plugin work. The default Impulse-first path must run with
only Python and SQLite.
Use `fala db init --db .fala/state.sqlite`, `fala db migrate --db ...`, and
`fala db status --db ...` for local schema setup and inspection.

## Impulse Package Schema

Fala package schema has a canonical model in `FalaPackageSpec` and a
loader in `fala.yaml_loader.load_fala_package_yaml`. Impulse fields
are parsed only by the Fala package loader.

```yaml
version: "2"
id: example_correlation_path

impulse_types:
  - id: input_text
    media_types: [text/plain]

association_kinds:
  - id: text_stats

reaction_kinds:
  - id: normalized_text
    media_types: [text/plain]

capabilities:
  - id: normalize
    accepts_impulse_types: [input_text]
    emits_reaction_kinds: [normalized_text]
    emits_association_kinds: [text_stats]

correlation_paths:
  - id: basic
    effectors:
      - id: normalize
        capability: normalize
        adapter:
          kind: python_function
          ref: examples.effectors.normalize_text

runtime:
  backend:
    kind: sqlite
    path: .fala/state.sqlite
  reaction_store:
    kind: filesystem
    root: .fala/reactions
```

## Embedded Driver

The claim/execute/complete loop behind `fala run-until-idle` is a library API
in `fala.driver`, so embedded consumers drive a run in-process instead of
shelling out to the CLI:

```python
from threading import Event

from fala import RuntimeBackendService, run_until_idle

service = RuntimeBackendService.sqlite(".fala/state.sqlite")
stop_event = Event()
result = await run_until_idle(
    service,
    worker_id="embedded",
    run_id="run_case",
    lease_seconds=300.0,
    max_ticks=100,
    should_stop=stop_event.is_set,
)
assert result.stopped_reason in {"idle", "max_ticks", "stopped"}
```

`run_until_idle` returns a `RunUntilIdleResult` with the typed `completed`,
`failed`, and `waiting` process lists. Pass `should_stop` to stop claiming new
processes between ticks; the in-flight effector completes, then the result uses
`stopped_reason="stopped"`. The CLI `run-until-idle` command is a thin wrapper
over this function; both share the same adapter dispatch, homeostat-wait handling,
retry/fail transitions, and `fala_runtime` bridge enqueueing. The same
entrypoint is exposed as `AutonomousCorrelator.run_until_idle`.

## CorrelationPath Orchestration

`fala.correlation_paths` executes a `CorrelationPathSpec` dependency graph over the process
store:

- `instantiate_correlation_path(service, run_id=..., correlation_path=..., effector_inputs=..., ...)`
  schedules one process per correlation_path effector. Effectors with no `conduction` start `ready`;
  effectors with `conduction` start `pending`, which is invisible to claim.
- After each successful effector, the driver readies dependent effectors whose conduction
  have all succeeded, injecting each upstream effector's output into the dependent effector's
  input under `"conduction"` (readable via `fala.sdk.conduction`). Explicit re-evaluation
  is available through `advance_correlation_path(service, run_id=..., correlation_path_id=...)`.
- An effector whose conduction can no longer succeed (an upstream was cancelled, timed out,
  or failed with no attempts left) is never readied and never auto-cancelled:
  `pending` is unclaimable, so blocked effectors fail closed by inaction and are
  reported in `CorrelationPathAdvance.blocked`.

```python
from fala import instantiate_correlation_path, run_until_idle

instance = await instantiate_correlation_path(
    service,
    run_id="run_case",
    correlation_path=package.correlation_paths[0],
    effector_inputs={"normalize": {"value": "raw text"}},
)
result = await run_until_idle(service, worker_id="embedded", run_id="run_case")
```

CorrelationPath membership is recorded in each process's `metadata["correlation_path"]` marker
(`correlation_path_id`, `correlation_path_spec_id`, `effector_id`, `conduction`); instantiation and readying
are idempotent through deterministic command idempotency keys. The same
entrypoints are exposed as `AutonomousCorrelator.instantiate_correlation_path` and
`AutonomousCorrelator.advance_correlation_path`.

## Conformance

Reusable backend conformance checks live in
`tests/runtime_backend_conformance.py`. The shipped SQLite backend runs those
checks in `tests/test_runtime_backend_conformance.py`.

The conformance checks cover:

- transactional impulse acceptance and impulse persistence;
- transactional run creation, run persistence, and status transitions;
- transactional impulse type/relation/association/reaction/process/homeostat/projection
  mutation and persistence;
- idempotent command submission;
- ordered command-linked events;
- associations, reactions, homeostats, and projections;
- manual homeostat completion, cancellation, and expiry through command/audit events;
- rebuilding the built-in `run_summary` projection from SQLite state and events;
- resource accounting fields in `run_summary`;
- process scheduling, atomic claim/lease, retry, fail, and completion;
- bridge inbox/outbox persistence;
- bridge runtime hop, impulse, and attempt budget enforcement;
- SQLite `schema_migrations` version marker.

## CLI Inspection

Impulse-first SQLite state can be inspected without FastAPI or a web server:

```bash
uv run fala create-run --db /tmp/fala-impulse.sqlite --run-id run_case --title "Case run"
uv run fala runs inspect --db /tmp/fala-impulse.sqlite --run-id run_case
uv run fala runs cancel --db /tmp/fala-impulse.sqlite --run-id run_case --reason "operator requested"
uv run fala commands list --db /tmp/fala-impulse.sqlite --run-id run_case
uv run fala commands inspect --db /tmp/fala-impulse.sqlite --run-id run_case --command-id command_123
uv run fala impulses list --db /tmp/fala-impulse.sqlite --run-id run_case
uv run fala impulses inspect --db /tmp/fala-impulse.sqlite --run-id run_case --impulse-id impulse_case
uv run fala impulse-types list --db /tmp/fala-impulse.sqlite --run-id run_case
uv run fala impulse-relations list --db /tmp/fala-impulse.sqlite --run-id run_case --impulse-id impulse_case
uv run fala reactions list --db /tmp/fala-impulse.sqlite --run-id run_case --impulse-id impulse_case
uv run fala processes list --db /tmp/fala-impulse.sqlite --run-id run_case --status ready
uv run fala processes cancel --db /tmp/fala-impulse.sqlite --run-id run_case --process-id process_123
uv run fala processes timeout --db /tmp/fala-impulse.sqlite --run-id run_case --process-id process_123
uv run fala events list --db /tmp/fala-impulse.sqlite --run-id run_case
uv run fala events validate-schema --db /tmp/fala-impulse.sqlite --run-id run_case --max-schema-version 1
uv run fala associations list --db /tmp/fala-impulse.sqlite --run-id run_case
uv run fala homeostats list --db /tmp/fala-impulse.sqlite --run-id run_case
uv run fala homeostat complete --db /tmp/fala-impulse.sqlite --run-id run_case --homeostat-id homeostat_review --value decision=approved
uv run fala homeostat cancel --db /tmp/fala-impulse.sqlite --run-id run_case --homeostat-id homeostat_review --value reason=operator
uv run fala homeostat expire --db /tmp/fala-impulse.sqlite --run-id run_case --homeostat-id homeostat_review --value reason=timeout
uv run fala projections list --db /tmp/fala-impulse.sqlite --run-id run_case
uv run fala projections rebuild --db /tmp/fala-impulse.sqlite --run-id run_case
uv run fala doctor --db /tmp/fala-impulse.sqlite
uv run fala bridge list --db /tmp/fala-source.sqlite --run-id run_case
uv run fala bridge deliver --db /tmp/fala-source.sqlite --run-id run_case --delivery-id bridge_1 --target-db /tmp/fala-target.sqlite
uv run fala trace --db /tmp/fala-impulse.sqlite --run-id run_case
uv run fala export-html --db /tmp/fala-impulse.sqlite --run-id run_case --out report.html
uv run fala export-bundle --db /tmp/fala-impulse.sqlite --run-id run_case --out run_case.fala.zip
```

## Local Examples

Run the local-first Impulse runtime example:

```bash
uv run python examples/runtime/local_first.py /tmp/fala-impulse.sqlite
```

The example uses one local SQLite file and exercises an impulse, association,
homeostat, and projection.
