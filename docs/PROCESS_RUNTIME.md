# Fala Runtime

Fala is a control layer for document workflow execution. It does not transform
documents itself. Step programs do that.

## Boundary

Framework owns:

- workflow package loading
- graph validation
- process status state
- queue claims and leases
- retry and timeout policy
- event log
- artifact refs
- combineLatest projections
- HTTP client, worker runner, CLI, optional FastAPI router

Step program owns:

- parsing
- enrichment
- normalization
- calls to models or external systems
- output values and artifacts

## Pipeline YAML

```yaml
pipeline: basic_enrichment
steps:
  - id: ingest
    adapter:
      kind: subprocess
      command: ["python", "steps/ingest.py"]
      cwd: "."
  - id: enrich
    needs: [ingest]
    adapter:
      kind: queue
      queue: basic.enrich
combines:
  - id: document_result
    needs: [ingest, enrich]
```

Every step is a program boundary:

- `subprocess`: Fala starts a local executable and passes context over stdin
- `http`: Fala posts context to an external service
- `queue`: external workers claim work and write output through the API

## Work Item Claim Policy

Pipelines can choose how ready work items are claimed. Fala does not attach
business meaning to a work item; the host application decides whether it
represents a PDF, image, record, batch, tenant job, or another unit of work.

```yaml
pipeline: ordered_workflow
work_items:
  claim_strategy: sequential
  order_by: index
steps:
  - id: process
    adapter:
      kind: queue
      queue: ordered.process
```

Supported strategies:

- `parallel` (default): workers may claim ready processes from different work
  items independently.
- `sequential`: workers only claim from the first non-terminal work item,
  ordered by the initial input value named by `order_by`.

## Package YAML

```yaml
package: basic_examples
pipelines:
  - basic_enrichment.yaml
workers:
  - id: enrich_worker
    pipeline: basic_enrichment
    process: enrich
    command: ["python", "steps/enrich.py"]
    cwd: "."
```

## FastAPI Integration

```python
from fastapi import FastAPI
from fala import PipelineRegistry, RuntimeService, SQLiteStateStore, create_runtime_router

registry = PipelineRegistry.from_directory("examples/pipelines")
store = SQLiteStateStore("runtime.db")
service = RuntimeService(registry=registry, store=store)

app = FastAPI()
app.include_router(create_runtime_router(service), prefix="/api")
```

Host app owns auth and tenancy. Pass `ensure_run_access` to
`create_runtime_router` when access checks are needed.
