# Fala

Fala is a small Python framework for observable document workflows.

It manages graph state, process claims, leases, retries, timeouts, events,
artifacts, and combineLatest-style projections. It does not own business logic.
Each workflow step is a separate program, HTTP service, or queue worker.

## Shape

- `fala`: core runtime models, scheduler, stores, adapters, client, CLI, worker
- `fala.sdk`: dependency-light helpers for step programs
- `RuntimeService`: host-side service facade over a registry and store
- `create_runtime_router`: optional FastAPI router for API integration
- YAML packages define pipelines and worker commands

## Quick check

```bash
uv run fala --pipeline-dir examples/pipelines validate --json --check-commands
uv run python -m unittest discover -s tests
```

## Run example

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  init-document \
  --db /tmp/fala-example.db \
  --pipeline basic_enrichment \
  --run-id run_1 \
  --document-id doc_1 \
  --value source=sample.txt

uv run fala \
  --pipeline-dir examples/pipelines \
  run-until-idle \
  --db /tmp/fala-example.db \
  --pipeline basic_enrichment \
  --run-id run_1 \
  --worker-id local \
  --adapter-kind subprocess
```

## Step contract

Step program reads one JSON `ProcessExecutionContext` from stdin and writes one
JSON `ProcessOutput` to stdout. Progress events go to stderr with the
`PROCESS_RUNTIME_EVENT ` prefix. `fala.sdk.run_stdio` handles this boilerplate.
