# Fala

Fala is a small Python framework for observable document workflows.

It manages graph state, process claims, leases, retries, timeouts, events,
artifacts, and combineLatest-style projections. It does not own business logic.
Each workflow step is a separate program, HTTP service, queue worker, or manual
operator gate.

## Shape

- `fala`: core runtime models, run/document registry, scheduler, stores,
  adapters, client, CLI, worker
- `fala.sdk`: dependency-light helpers for step programs
- `RuntimeService`: host-side service facade over a registry and store
- `create_runtime_router`: optional FastAPI router for API integration
- `SQLiteStateStore` for local/single-host use, `PostgresStateStore` for a
  shared control plane
- YAML packages define document types, artifact kinds, capabilities, pipelines,
  and worker commands

## Quick check

```bash
uv run fala --pipeline-dir examples/pipelines validate --json --check-commands
uv run fala --pipeline-dir examples/pipelines package-doctor
uv run python -m unittest discover -s tests
```

Commands that accept `--db` use SQLite for filesystem paths and Postgres for
`postgresql://...` DSNs. The web app uses `FALA_DATABASE_URL`, then `FALA_DB`,
then `fala.db`.
Use `db-doctor` before starting a shared control plane:

```bash
uv run fala db-doctor --db runtime.db --ensure-schema
uv run fala db-doctor --db "$FALA_DATABASE_URL" --ensure-schema
uv run fala project-check --project-dir . --db runtime.db --ensure-schema --output project-check.json
uv run fala project-smoke --project-dir . --db runtime.db --run-id run_smoke --output project-smoke.json
```

`db-doctor` reports store kind, runtime schema table coverage, current/latest
schema version, applied migrations, missing migrations, and row counts. Without
`--ensure-schema` it checks the target as-is; with `--ensure-schema` it creates
or repairs the runtime schema before reporting.

Run the bundled API and web panel:

```bash
uv run fala --pipeline-dir examples/pipelines \
  serve \
  --db fala.db \
  --host 127.0.0.1 \
  --port 8000
```

Generate a runnable local stack with the web/API control plane, shared volumes,
optional Postgres, and package workers:

```bash
uv run fala --pipeline-dir examples/pipelines \
  deployment \
  --format docker-compose \
  --run-id example-run \
  --with-postgres \
  | jq -r .manifest > docker-compose.yaml
```

ASGI deployments can use `fala.web.asgi:app`; configure `FALA_PIPELINE_DIR`,
`FALA_DATABASE_URL` or `FALA_DB`, `FALA_QUEUE_BROKER` or `FALA_QUEUE_DB`, and
`FALA_ARTIFACT_STORE` or `FALA_ARTIFACT_STORE_ROOT` through the environment.

Set `FALA_POSTGRES_TEST_DSN` and run with `--extra postgres` to exercise the
live Postgres store contract.

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

Batch import:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  create-run \
  --db /tmp/fala-example.db \
  --pipeline basic_enrichment \
  --run-id run_batch_1 \
  --existing-run resume \
  --existing-document reuse \
  --file ./document.txt \
  --document email_1=s3://bucket/email_1.eml
```

Batch import can also come from one JSON/YAML manifest:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  validate-run \
  --run-input examples/pipelines/basic/run-input.example.yaml

uv run fala \
  --pipeline-dir examples/pipelines \
  create-run \
  --db /tmp/fala-example.db \
  --run-input examples/pipelines/basic/run-input.example.yaml

uv run fala discover-documents \
  --input-dir ./incoming \
  --pipeline basic_enrichment \
  --run-id run_batch_1 \
  --document-type generic_document \
  --content-hash \
  > ./run-input.json

uv run fala discover-documents \
  --source-list ./sources.csv \
  --route ./document-routes.yaml \
  --pipeline basic_enrichment \
  --run-id run_batch_1 \
  > ./run-input.json

uv run fala \
  --pipeline-dir examples/pipelines \
  validate-run \
  --run-input ./run-input.json

uv run fala \
  --pipeline-dir examples/pipelines \
  plan-run \
  --run-input ./run-input.json

uv run fala inspect-run-input \
  --run-input ./run-input.json

uv run fala \
  --pipeline-dir examples/pipelines \
  create-run \
  --db /tmp/fala-example.db \
  --run-input ./run-input.json
```

Batch documents are persisted as runtime records with title/type/media/source
metadata, lifecycle status, and summary counts. API:

```text
GET /api/runs/{run_id}/process-runtime/documents
```

Use query params for large runs: `status`, `pipeline_id`, `document_type`,
`relation`, `parent_document_id`, `limit`, and `offset`. CLI:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  list-documents \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --status queued \
  --limit 100
```

Process instances are also pageable:

```text
GET /api/runs/{run_id}/process-runtime/processes
```

Supported query params: `status`, `pipeline_id`, `document_type`,
`parent_document_id`, `document_id`, `process_id`, `capability`,
`operation_type`, `adapter_kind`, `resource_pool`, `limit`, and `offset`.

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  list-processes \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --status queued \
  --operation-type ingest \
  --capability ingest_document \
  --limit 100
```

Failed terminal processes are exposed as a runtime dead-letter queue. It is a
read model over process status/events, so operators can triage generic document
work without knowing every pipeline shape:

```text
GET  /api/runs/{run_id}/process-runtime/dead-letter
POST /api/runs/{run_id}/process-runtime/dead-letter/{document_id}/processes/{process_id}/replay
GET  /api/runs/{run_id}/process-runtime/stuck-work
```

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  dead-letter \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --operation-type ingest

uv run fala \
  --pipeline-dir examples/pipelines \
  replay-dead-letter \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --document-id doc_1 \
  --process-id extract \
  --reason "fixed source"

uv run fala \
  --pipeline-dir examples/pipelines \
  stuck-work \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --operation-type extract \
  --queued-after-seconds 600 \
  --running-after-seconds 1800
```

When a step declares `sla`, those per-step thresholds override the
`stuck-work` query defaults for queued, waiting, and running status checks.

Capability-level worker demand:

```text
GET /api/runs/{run_id}/process-runtime/capability-demands
```

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  capability-demands \
  --db /tmp/fala-example.db \
  --run-id run_batch_1
```

The web panel includes `/project` for workspace readiness,
`/project/bootstrap` for operator bootstrap checks, DB schema status, and
ready-to-run commands, `/project/spec` for one bootstrap runbook/spec,
`/project/operations` for project-level health, backlog, worker demand, and
supervision summaries,
`/project/alerts` for editable project policy rules over operations metrics,
`/project/lifecycle` for run-retention and artifact-GC planning,
`/project/supervision` for project-level dead-letter, stuck-work, and stream-lag
triage, and `/runs/new` for creating a batch run from uploaded files, document
ids, URIs, or local server paths. Uploaded files are stored as content-addressed
`fala-artifact://sha256/...` source refs. The run detail page loads operator
partials for runtime state, dead-letter replay, stuck-work triage, run
provenance, manual queue, run results, output documents, reductions, document
lineage, and operator audit:

```text
GET  /project
GET  /project/bootstrap
GET  /project/spec
GET  /project/operations
GET  /project/alerts
GET  /project/lifecycle
GET  /project/supervision
POST /project/runs
GET  /runs/{run_id}/process-runtime
GET  /runs/{run_id}/process-runtime/documents
GET  /runs/{run_id}/process-runtime/processes
GET  /runs/{run_id}/process-runtime/dead-letter
POST /runs/{run_id}/process-runtime/dead-letter/{document_id}/processes/{process_id}/replay
GET  /runs/{run_id}/process-runtime/stuck-work
POST /runs/{run_id}/process-runtime/stuck-work/{document_id}/processes/{process_id}/actions/{action}
GET  /runs/{run_id}/process-runtime/capability-demands
GET  /runs/{run_id}/process-runtime/provenance
GET  /runs/{run_id}/process-runtime/events
GET  /runs/{run_id}/process-runtime/manual
POST /runs/{run_id}/process-runtime/{document_id}/processes/{process_id}/manual-complete
GET  /runs/{run_id}/process-runtime/results
GET  /runs/{run_id}/process-runtime/output-documents
GET  /runs/{run_id}/process-runtime/reductions
GET  /runs/{run_id}/process-runtime/lineage
GET  /runs/{run_id}/process-runtime/audit
GET  /process-runtime/blueprints
GET  /api/process-runtime/project
GET  /api/process-runtime/project/bootstrap
GET  /api/process-runtime/project/spec
GET  /api/process-runtime/project/runs
GET  /api/process-runtime/project/operations
GET  /api/process-runtime/project/alerts
GET  /api/process-runtime/project/lifecycle
POST /api/process-runtime/project/lifecycle
GET  /api/process-runtime/project/supervision
POST /api/process-runtime/project/runs
GET  /api/process-runtime/blueprints
GET  /api/process-runtime/blueprints/{blueprint_id}
```

Use `--existing-run resume --existing-document reuse` for resumable imports.
Fala reuses the existing run and existing document input without overwriting
record metadata, then schedules any still-ready work. Default policy is `error`.

Append another batch to an existing run without touching run metadata:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  append-documents \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --pipeline basic_enrichment \
  --existing-document reuse \
  --document email_2=s3://bucket/email_2.eml
```

```text
POST /api/runs/{run_id}/process-runtime/documents/batch
```

New batch runs also record a provenance snapshot under
`run.metadata.process_runtime.run_provenance`: run input digest, pipeline contract
digests, compact dry plan, document summary, and the pipeline contracts used at
creation time. The snapshot excludes the per-document plan list, so large
batches do not inflate run metadata. Operators can fetch the normalized
snapshot through `GET /api/process-runtime/runs/{run_id}/provenance`. That
response also compares stored pipeline contracts with the current registry and
returns `contract_drift`, so operators can see whether replay/resume would run
under the same contract or a changed one.
Later `append-documents` calls add compact `append_batches` entries with input
hashes, document summaries, schedule summaries, and routed append reports when
routing was used.

## Step contract

Step program reads one JSON `ProcessExecutionContext` from stdin and writes one
JSON `ProcessOutput` to stdout. Progress events go to stderr with the
`PROCESS_RUNTIME_EVENT ` prefix. `ProcessOutput.stream_chunks` can carry ordered
chunk payloads that Fala persists beside final output. `fala.sdk.run_stdio`
handles this boilerplate.

## Generic workflow contract

Packages can declare the reusable document-processing vocabulary:

- `document_types`: input classes such as PDF, email, spreadsheet, image, video,
  or any domain-specific document type
- `document_relations`: typed edges between documents, such as `page`,
  `attachment`, `redacted`, `translated`, or `rendered`
- `operation_types`: reusable categories of work such as `ingest`, `extract`,
  `split`, `generate`, `review`, `index`, or `export`
- `artifact_kinds`: typed intermediate/output products such as extracted text,
  normalized JSON, rendered image, generated script, or video segment
- `capabilities`: reusable units of work that accept document/artifact kinds and
  emit child/output document types, artifact kinds, streams, or typed JSON values
- `secrets`: named external secret refs; Fala stores ids/env names, never values
- worker `sandbox`: deployment policy such as non-root, read-only root FS, no
  privilege escalation, dropped Linux capabilities

Pipeline steps reference capabilities with `capability: ...`. Workers can
declare supported capabilities. Capabilities can reference `operation_type`, so
operators and bootstrap tools can group process work independently from local
step names. Runtime process records, state summaries, queue metrics,
dead-letter, stuck-work, stream-lag, and run results expose and filter by that
operation type. Fala validates references and simple typed flow inside a package:
step config must match `config_schema`, root steps must accept document types,
and dependent steps must accept artifacts emitted by their needs.
Workers can reference declared secrets; deployment manifests map them to
Docker/Compose environment placeholders or Kubernetes `secretKeyRef` entries.
Runtime batch inputs with
`document_type` are checked against the package and pipeline root capabilities;
document values, metadata, media type, and extension are checked against the
document type contract; runtime output artifacts are checked against the step
capability's declared `emits_artifact_kinds` and artifact kind metadata/format
contract. `output_documents` are checked against package document types,
declared `document_relations`, and the capability's declared
`emits_document_types`; use them for produced documents such as redacted PDFs,
translated DOCX files, rendered videos, or generated scripts. Spawned child
documents are checked against package document types, declared
`document_relations`, and, when declared, the capability's
`emits_document_types`.
Stream chunks are checked against capability `emits_streams` when declared. When
an artifact kind declares `value_schema`, the artifact URI must resolve to UTF-8
JSON that matches that schema. Output `values` are checked against the
capability `output_schema` when present.
Stored process outputs also include `metadata.process_runtime.lineage` with input
artifact summaries, dependency output summaries, value keys, process id, document
id, capability, attempt, and worker id when known.
Business logic still stays outside the framework.

Example worker secret and sandbox policy:

```yaml
secrets:
  - id: openai_api_key
    env_var: OPENAI_API_KEY
    kubernetes_secret_name: fala-openai
    kubernetes_secret_key: api-key
workers:
  - id: ocr_worker
    capabilities: [extract_text]
    pipeline: document_processing
    process: ocr
    command: ["python", "steps/ocr.py"]
    secrets: [openai_api_key]
    sandbox:
      run_as_non_root: true
      read_only_root_filesystem: true
      allow_privilege_escalation: false
      drop_capabilities: [ALL]
```

Pipeline steps can also declare execution policy:

```yaml
steps:
  - id: ocr
    capability: extract_text
    priority: 50
    max_concurrency: 4
    resource_pool: gpu_pool
    resources:
      gpu_count: 1
      memory_mb: 8192
      labels: [cuda]
      units:
        ocr_slots: 2
    retry:
      max_attempts: 3
      delay_seconds: 30
      retry_error_kinds: [transient_io, rate_limited]
      terminal_error_kinds: [validation_error, permission_denied]
    sla:
      queued_after_seconds: 600
      waiting_after_seconds: 3600
      running_after_seconds: 1800
    adapter:
      kind: queue
      queue: documents.ocr
```

Higher `priority` steps are claimed first. `max_concurrency` caps concurrent
claims for the same process within a run, so expensive OCR, LLM, render, or video
steps can be throttled without hard-coding domain logic. `retry.delay_seconds`
keeps failed or expired attempts in backoff before the next claim. Workers can
send an `error_kind`; `terminal_error_kinds` fail immediately, while a non-empty
`retry_error_kinds` list retries only those classified failures.

`sla` is operator policy for stuck-work detection. `stuck-work` uses per-step
thresholds first, then falls back to CLI/API query defaults. This lets a slow
video render and a fast metadata extraction step live in the same generic run
without hard-coded process names.

`resources` gates claims generically. Steps can require CPU, memory, disk, GPU,
labels, or named numeric units. Workers declare available resources through
heartbeats, package worker manifests, or worker CLI flags. Fala only claims a
step when the worker satisfies those requirements, and queue metrics report the
same mismatch as `missing_worker`.

Human-in-the-loop steps use `adapter.kind: manual`. They become `waiting` with a
`process.manual_required` event, are never claimed by workers, and unblock
downstream steps when an operator writes a normal `ProcessOutput`:

```yaml
steps:
  - id: review
    capability: approve_document
    adapter:
      kind: manual
  - id: publish
    needs: [review]
    adapter:
      kind: queue
      queue: documents.publish
```

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  complete-process \
  --db /tmp/fala-example.db \
  --pipeline document_processing \
  --run-id run_batch_1 \
  --document-id doc.pdf \
  --process-id review \
  --value approved=yes
```

Conditional steps use `when` for coarse document routing. This keeps one
pipeline usable for heterogeneous batches without domain-specific scheduler
logic. Supported matches are exact `document_types`, wildcard-capable
`media_types`, and exact document `metadata` / initial `values` key matches.
Non-matching steps are `skipped`; dependents of skipped steps are skipped too.

```yaml
steps:
  - id: parse_pdf
    when:
      document_types: [pdf_document]
    adapter:
      kind: queue
      queue: documents.pdf
  - id: parse_email
    when:
      document_types: [email_document]
      media_types: ["message/*"]
    adapter:
      kind: queue
      queue: documents.email
```

Processes can fan out by returning `spawn_documents` in `ProcessOutput`. This is
for pages, attachments, chunks, media segments, or any derived document. Fala
records `parent_document_id` and `parent_process_id`, initializes each child in
the same run, and schedules it using the child `pipeline_id`. When the child
omits `pipeline_id`, Fala first auto-routes it from workflow package document
contracts (`document_type`, media type, extension), then falls back to the parent
pipeline if nothing matches. Output metadata records
`process_runtime.spawn_route_report` for that decision.
Typed packages can declare allowed child outputs with capability
`emits_document_types`; Fala validates spawned documents against that contract
and declared `document_relations` after routing.

Parent steps can fan in after spawned child work by setting
`wait_for_children` on a process. The step still uses normal `needs`, then waits
for matching child documents to reach the required document statuses:

```yaml
steps:
  - id: assemble
    needs: [split]
    wait_for_children:
      from_processes: [split]
      document_types: [pdf_page]
      relations: [page]
      min_count: 1
      required_statuses: [completed]
    adapter:
      kind: queue
      queue: documents.assemble
```

Use `output_documents` when a process produces a document result but should not
schedule it as more work:

```json
{
  "values": {"status": "ok"},
  "artifacts": [
    {
      "id": "redacted_pdf",
      "kind": "redacted_file",
      "uri": "s3://bucket/redacted.pdf",
      "metadata": {"media_type": "application/pdf", "filename": "redacted.pdf"}
    }
  ],
  "output_documents": [
    {
      "id": "redacted_doc",
      "document_type": "redacted_document",
      "media_type": "application/pdf",
      "artifact_id": "redacted_pdf",
      "relation": "redacted",
      "values": {"source": "source.pdf"},
      "metadata": {"filename": "redacted.pdf"}
    }
  ]
}
```

```json
{
  "values": {"page_count": 1},
  "spawn_documents": [
    {
      "document_id": "case.pdf#page-1",
      "document_type": "pdf_page",
      "media_type": "application/pdf",
      "source_uri": "file:///tmp/case-page-1.pdf",
      "values": {"page_number": 1}
    }
  ]
}
```

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  complete-process \
  --db /tmp/fala-example.db \
  --pipeline document_processing \
  --run-id run_batch_1 \
  --document-id case.pdf \
  --process-id split \
  --output-file split-output.json
```

Document lineage is available as a run-level read model:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  document-lineage \
  --db /tmp/fala-example.db \
  --run-id run_batch_1
```

```text
GET /api/runs/{run_id}/process-runtime/document-lineage
```

Run results export process outputs across all documents without returning the
full runtime state:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  run-results \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --operation-type export \
  --process-id export
```

Use `--jsonl` for downstream indexing, reporting, or reduce jobs. The API
supports the same filters: `pipeline_id`, `process_id`, `document_id`, and
`document_type`, plus `operation_type`.

```text
GET /api/runs/{run_id}/process-runtime/results
```

Typed document products have a first-class read model, so generated PDFs,
translated DOCX files, rendered videos, scripts, and similar products can be
indexed without parsing the whole process output payload:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  output-documents \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --document-type redacted_document \
  --relation redacted
```

Supported filters: `pipeline_id`, `process_id`, `document_id` for the source
runtime document, `source_document_type`, `output_document_id`,
`document_type`, `relation`, and `media_type`.

```text
GET /api/runs/{run_id}/process-runtime/output-documents
```

Pipelines can declare run-level reductions over those result rows:

```yaml
reduces:
  - id: exported_documents
    process_id: export
    mode: collect_values
```

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  run-reductions \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --pipeline document_processing
```

Reduction modes: `collect_values`, `collect_outputs`, and `count`. The web
panel renders the same reductions on the run detail page, next to the manual
queue and compact result table.

Runs can also define resource pool quotas:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  create-run \
  --db /tmp/fala-example.db \
  --pipeline document_processing \
  --run-id run_gpu \
  --document doc_1=file:///tmp/doc_1.pdf \
  --resource-pool gpu_pool.gpu_count=1 \
  --resource-pool gpu_pool.memory_mb=16384 \
  --resource-pool gpu_pool.units.ocr_slots=2
```

If `gpu_pool` is saturated, matching workers stay healthy but new claims for
steps bound to that pool wait. Metrics report `resource_blocked_count` and
resource pool usage/remaining capacity.

Preflight a pipeline contract before creating a run:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  contract basic_enrichment
```

API equivalent:

```text
GET /api/process-runtime/pipelines/{pipeline_id}/contract
```

Export a versioned package release index with stable digests:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  package-index
```

The index includes package version, manifest SHA-256, pipeline model SHA-256,
pipeline contract SHA-256, worker ids, capability ids, and source paths. Use it
as a release lock for deployments, cache keys, rollout checks, or audit. API:

```text
GET /api/process-runtime/packages/index
GET /api/process-runtime/packages/{package_id}/release
```

Check whether a package is ready to bootstrap a real document workflow:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  package-doctor
```

The doctor report checks package contract coverage, routeable document types,
pipeline DAG artifact contracts, queue worker coverage, package worker command
availability, sample run input presence, runtime validation for sample
run/source-list manifests, source-list sample source files, unused
capabilities, artifact kinds/secrets, and blocking errors. The web panel shows
the same readiness summary on
`/process-runtime/pipelines`. For generated multi-package workspaces,
`fala-project.yaml` adds a root-level project check that covers workspace
files, package readiness, and mixed source-list routing; the panel shows it on
`/project` and the operator bootstrap view on `/project/bootstrap`. API:

```text
GET /api/process-runtime/project
GET /api/process-runtime/project/bootstrap
GET /api/process-runtime/project/spec
GET /api/process-runtime/project/runs
GET /api/process-runtime/project/operations
GET /api/process-runtime/project/alerts
GET /api/process-runtime/project/lifecycle
POST /api/process-runtime/project/lifecycle
GET /api/process-runtime/project/supervision
POST /api/process-runtime/project/runs
GET /api/process-runtime/packages/readiness
GET /api/process-runtime/packages/{package_id}/readiness
```

`GET /api/process-runtime/project/runs` accepts `status`, `package_id`,
`pipeline_id`, `document_type`, and `limit`, and returns project-level run
history with status, package, pipeline, and document-type counters.
`GET /api/process-runtime/project/bootstrap` returns the aggregate bootstrap
check, runtime DB schema diagnostics, and copyable `db-doctor`,
`project-check`, and `project-smoke` commands.
`GET /api/process-runtime/project/spec` returns one bootstrap runbook/spec with
project manifest, readiness, package release index, routes, source-list intake,
alert policy, lifecycle policy, secret inventory, web/API entry points, and
worker commands.
`GET /api/process-runtime/project/supervision` accepts `package_id`,
`pipeline_id`, `document_type`, `operation_type`, stuck-work thresholds,
stream-lag filters, and `limit`, and aggregates dead-letter, stuck-work, and
stream-lag items across
matching project runs.
`GET /api/process-runtime/project/operations` accepts the same project filters
plus `stale_after_seconds`, and aggregates run health, queue backlog, worker
deficits, capability demand, unhealthy-worker issues, and supervision counters.
`GET /api/process-runtime/project/alerts` evaluates `fala-project.yaml`
`alerts.rules` over the operations report. Each rule uses `metric`, `operator`,
`threshold`, `severity`, and `message`, so projects can tune alert policy
without code changes.
`GET /api/process-runtime/project/lifecycle` plans project-scoped run retention
from `fala-project.yaml` `lifecycle.run_retention` plus current artifact-GC
orphans. `POST /api/process-runtime/project/lifecycle` can delete only matching
project run state when `delete: true`; artifact GC stays a separate explicit
admin operation.

The same project views are available without running the web server:

```bash
uv run fala project-supervision --project-dir . --db runtime.db
uv run fala project-operations --project-dir . --db runtime.db
uv run fala project-alerts --project-dir . --db runtime.db
uv run fala project-lifecycle --project-dir . --db runtime.db --before 2026-01-01T00:00:00+00:00
uv run fala project-secrets --project-dir . --output project-secrets.json --env-output .env.example
uv run fala project-bundle --project-dir . --output fala-project-bundle.tar.gz
uv run fala project-bundle-verify fala-project-bundle.tar.gz
uv run fala project-check --project-dir . --db runtime.db --bundle fala-project-bundle.tar.gz
uv run fala project-smoke --project-dir . --db runtime.db --run-id run_smoke
```

`project-bundle` writes a portable `tar.gz` with `fala-project.yaml`,
route/source-list files, pipeline packages, generated `project-spec.json`,
`project-secrets.json`, `package-index.json`, `.env.example`, and
`bundle-manifest.json`. It excludes runtime DB files, artifact stores, `.env`,
and other local runtime/cache state. `project-bundle-verify` checks safe archive
paths, required files, manifest checksums, and absence of runtime DB or local
secret files.

Preflight a concrete batch before creating a run:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  validate-run \
  --pipeline basic_enrichment \
  --existing-run resume \
  --existing-document reuse \
  --document-type generic_document \
  --file ./documents/example.txt
```

API equivalent:

```text
POST /api/process-runtime/runs/validate
```

This validates document type, metadata schema, media type, extension, and root
pipeline acceptance, then returns matched pipeline contracts without writing run
or document state. Returned contracts include per-step capability schema,
artifact flow, and execution policy.

`validate-run` and `create-run` accept `--run-input path.yaml` for full
`RuntimeRunInput` manifests. CLI flags can override scalar fields and append
extra `--file` or `--document` entries. `validate-run` also returns
`document_summary` with counts by pipeline, document type, media type, source
scheme, artifact kind, and discovered value/metadata keys.
`plan-run` returns the same validation preview plus a dry execution plan:
per-document queued/waiting steps, process totals, resource pool requests, and
declared worker demand. It does not write run state.
`inspect-run-input` reads the raw manifest and reports duplicate `document_id`,
duplicate `source_sha256`, duplicate `source_uri`, and the same summary without
requiring the manifest to pass runtime validation first.

Bootstrap a package skeleton:

```bash
uv run fala scaffold \
  --output-dir ./pipelines/invoices \
  --package-id invoice_processing \
  --pipeline-id invoice_flow \
  --steps ingest,extract,review,export \
  --document-type invoice_document \
  --document-media-type application/pdf \
  --document-extension pdf \
  --artifact-extension export=json \
  --artifact-value-schema export=./schemas/invoice-export-artifact.yaml \
  --capability-output-schema export=./schemas/invoice-export-output.yaml \
  --stream-contract extract=./schemas/invoice-extract-streams.yaml \
  --step-policy review=./schemas/invoice-review-policy.yaml \
  --document-value-schema ./schemas/invoice-values.yaml \
  --document-metadata-schema ./schemas/invoice-metadata.yaml
```

Or start from a generic blueprint:

```bash
uv run fala scaffold-blueprints

uv run fala scaffold-blueprints --query "redacted document"

uv run fala scaffold-blueprints --blueprint generative_media
```

```python
from fala import get_scaffold_blueprint, list_scaffold_blueprints

catalog = list_scaffold_blueprints()
redaction_catalog = list_scaffold_blueprints(query="redacted_document")
blueprint = get_scaffold_blueprint("generative_media")
```

```bash
uv run fala scaffold \
  --output-dir ./pipelines/media \
  --package-id media_processing \
  --pipeline-id media_flow \
  --blueprint generative_media \
  --adapter-kind queue
```

For a heterogeneous workspace with several document/work types, initialize a
project from multiple blueprints:

```bash
uv run fala init-project \
  --output-dir ./document-workspace \
  --project-id document_workspace \
  --blueprint document_digitalization \
  --blueprint email_processing \
  --blueprint-file ./invoice-blueprint.yaml \
  --blueprint llm_document_processing \
  --adapter-kind queue

cd ./document-workspace
make bootstrap
make serve
```

`--blueprint` uses a built-in template. `--blueprint-file` includes a custom
YAML blueprint with the same schema accepted by `fala scaffold --blueprint-file`;
both flags are repeatable and can be mixed. If neither flag is passed,
`init-project` includes every built-in blueprint. The generated root has
`pipelines/<blueprint>/...` packages plus a root `fala-project.yaml` manifest
and Makefile for whole-workspace `validate`, `doctor`, `project-doctor`, package
bootstrap, and web/API startup. The manifest also includes editable
`alerts.rules` over operations metrics and `lifecycle.run_retention` for
project-scoped cleanup planning. It writes a root `source-list.example.csv` that
references one sample document from each package and
`document-routes.example.yaml` as editable intake policy.
`make project-doctor` checks root samples, package readiness, and mixed routing
from `fala-project.yaml`. `make db-doctor` checks the runtime database target
and creates/repairs the local schema when `DB` points at an empty SQLite file or
Postgres DSN; the report includes runtime schema version and applied migrations.
`make project-check` writes one aggregate bootstrap report over project
readiness, spec generation, secret inventory, DB readiness, and optional
bundle verification. `make project-smoke` creates the mixed sample run and
executes local subprocess steps plus declared queue worker commands until the
run completes or the smoke report captures the first failure. `make mixed-source-list`
compiles that source list with `--route document-routes.example.yaml` and
`--auto-route` fallback into one mixed run input, and `make create-mixed` uses
`fala create-project-run` to create that run with project metadata.
`make project-spec` writes the same project runbook to
`project-spec.json`. `make project-secrets` writes package worker secret
inventory to `project-secrets.json` and a local `.env.example` template without
storing secret values. `make project-bundle` writes a portable archive that
omits runtime DBs, artifact stores, and real `.env` values. The
`project-bundle-verify` target validates the archive before handoff. The
`project-supervision`, `project-operations`, `project-alerts`, and
`project-lifecycle` targets write the same headless operator reports as JSON. The
same root Makefile also has `make package-index`, `make worker-commands`, and
`make deployment-compose` for release digests, worker command rendering, and
local deployment manifests.

Built-in blueprints: `document_digitalization`, `email_processing`,
`document_package_processing`, `document_redaction_review`,
`document_translation_review`, `generative_media`, `llm_document_processing`,
`knowledge_base_ingestion`, `structured_extraction_review`, and
`tabular_data_processing`. They cover OCR/digitalization, mail processing,
archive or folder package fan-out, redaction/anonymization with review,
translation/localization with review, RAG or knowledge-base ingestion, structured
field extraction with human review, LLM answer generation, AI
media/script/image/video style workflows, and CSV/XLS/XLSX/JSON table
processing. They prefill document type, media types, file extensions, step ids,
artifact kinds, capability ids, child document output contracts, artifact media
types, artifact value schemas, capability output schemas, and common stream
contracts. They also prefill conservative step policy such as retry, SLA,
priority, max concurrency, resource pools, child wait barriers, and manual
review gates where the workflow shape calls for them.
`document_digitalization` is parent/page aware: root documents run ingest and
extract, wait on page children in assemble, and emitted `page_document` children
run ingest, normalize, enrich, and export while skipping the split/join steps.
`email_processing` is message/attachment aware: message documents can emit
`email_attachment_document` children, and attachment children run
ingest/classify/export while skipping MIME parse and attachment extraction.
`document_package_processing` is package/item aware: archive or folder roots run
inspect/extract/manifest export, while emitted `packaged_document` children run
ingest/classify/route/export.
`document_redaction_review` is redaction aware: source documents run text
extraction, sensitive span detection, redaction, manual review, and export while
emitting a `redacted_document` output contract.
`document_translation_review` is translation aware: source documents run text
extraction, segmentation, translation, manual review, assembly, and export while
emitting a `translated_document` output contract.
`scaffold-blueprints` returns the same catalog as JSON, including
step-to-capability/artifact mappings, DAG `needs`, accepted/emitted document
types, streams, resource pools, manual gates, and a starter scaffold command.
The web panel shows the same catalog at `/process-runtime/blueprints`, and the
API exposes it at
`/api/process-runtime/blueprints`. Use `--query`, API query parameter `query`,
or `/process-runtime/blueprints?query=...` to filter presets by document type,
media type, operation, step, capability, stream, relation, resource pool, or
common workflow words such as `email`, `redact`, `translation`, `csv`, `video`,
or `package item`. Use
`--document-extension` to set accepted
file extensions, `--artifact-extension STEP=EXT` to set accepted output artifact
extensions, `--artifact-value-schema STEP=PATH`,
`--capability-output-schema STEP=PATH`, and `--stream-contract STEP=PATH` to
attach typed per-step contracts. Use `--step-policy STEP=PATH` to attach
process policy such as manual review gates, retry, SLA, priority, resource
requirements, `when` routing, and per-step config. Blueprint defaults avoid hard
worker resource requirements so generated samples stay locally runnable; add
those requirements with `--step-policy` when needed. For blueprints, step policy
files are deep-merged into defaults, so adding `resources` or changing
`retry.max_attempts` does not discard default SLA, priority, resource pool, or
retry backoff. Use
`--document-value-schema` / `--document-metadata-schema` with JSON/YAML schema
files to define or override the generated document input contract. Generated
SDK step programs emit sample payloads that satisfy those schemas, so the
scaffold remains executable.
Generated files are normal package files.

Projects can also keep their own blueprint YAML files and pass them to Fala
without changing Fala source:

```yaml
id: invoice_review
title: Invoice review
document:
  type: invoice_document
  media_types: [application/pdf]
  extensions: [.pdf]
  value_schema:
    type: object
    properties:
      vendor:
        type: string
additional_document_types:
  - id: invoice_page
    media_types: [application/pdf]
    extensions: [.pdf]
additional_document_relations:
  - id: page
    source_document_types: [invoice_document]
    target_document_types: [invoice_page]
operation_types:
  - id: approve
    category: quality
steps:
  - id: ingest
    capability: ingest_invoice
    artifact_kind: source_invoice
    accepts_document_types: [invoice_document, invoice_page]
    artifact_media_types: [application/json]
  - id: extract
    capability: extract_invoice_fields
    artifact_kind: extracted_invoice
    emits_document_types: [invoice_page]
    streams:
      - stream: pages
        kinds: [page]
        value_schema:
          type: object
          properties:
            page_number:
              type: integer
  - id: approve
    capability: approve_invoice
    operation_type: approve
    artifact_kind: approved_invoice
    needs: [extract]
    policy:
      manual:
        required: true
  - id: export
    capability: export_invoice
    artifact_kind: exported_invoice
    needs: [extract, approve]
```

```bash
uv run fala scaffold-blueprints --blueprint-file ./invoice-blueprint.yaml

uv run fala scaffold \
  --output-dir ./pipelines/invoices \
  --package-id invoice_processing \
  --pipeline-id invoice_flow \
  --blueprint-file ./invoice-blueprint.yaml \
  --adapter-kind queue
```

Custom blueprint fields match the generated runtime contract surface: document
type/media/extensions/value schema/metadata schema, extra document types,
extra document relations, operation types, per-step accepted/emitted document
types, step ids, DAG `needs`, capabilities, artifact kinds, optional artifact
media/extensions/value schemas, capability output schemas, stream specs, step
policy, implementation guidance, and child wait barriers such as
`policy.wait_for_children`.

The scaffold writes `process-runtime-package.yaml`, pipeline YAML,
`run-input.example.yaml`, `source-list.example.csv`, an `incoming/` sample source
file, `README.scaffold.md`, `Makefile`, `contracts/` editable schema/policy
templates, SDK-backed step programs, document/artifact/capability declarations,
document input value schemas, starter JSON Schema contracts, stream contracts,
and queue worker manifests when `--adapter-kind queue` is used. The generated
README summarizes the contract surface, worker guidance, step policy, worker
mode, manual gates, and operator commands. Each generated `steps/*.py` file
contains `WORKER_GUIDANCE` with the step role, operation type, expected inputs,
outputs, streams, and what to replace in the sample implementation. The
generated Makefile gives the same bootstrap path as targets: `make bootstrap`,
`make create`, `make run-local`, `make serve`, and `make worker`. The sample run
input is a normal `RuntimeRunInput` with
sample document `values` and `metadata` from the scaffolded schemas; validate it
or create the first run
with:

```bash
uv run fala --pipeline-dir ./pipelines/media \
  validate-run \
  --run-input ./pipelines/media/run-input.example.yaml

uv run fala --pipeline-dir ./pipelines/media \
  create-run \
  --db runtime.db \
  --run-input ./pipelines/media/run-input.example.yaml
```

For many documents, edit the generated `source-list.example.csv` and turn it
into a reusable manifest. The CSV includes `value.*` and `metadata.*` columns
from the scaffolded document type schemas, plus graph fields such as
`relation`, `parent_document_id`, and `parent_process_id`.

```bash
uv run fala discover-documents \
  --source-list ./pipelines/media/source-list.example.csv \
  --pipeline media_flow \
  --run-id run_media_batch \
  > ./run-input.json
```

For heterogeneous folders or source lists, use route rules to fill per-document
pipeline and document type before run creation:

```yaml
routes:
  - id: invoices
    match:
      extensions: [.pdf]
    set:
      pipeline_id: invoice_flow
      document_type: invoice_document
  - id: emails
    match:
      extensions: [.eml]
    set:
      pipeline_id: email_flow
      document_type: email_document
```

```bash
uv run fala discover-documents \
  --input-dir ./incoming \
  --route ./document-routes.yaml \
  --route-report ./route-report.json \
  --run-id run_mixed_batch \
  > ./run-input.json
```

`--route-report` keeps stdout as the reusable run manifest and writes matched /
rejected route diagnostics to a sidecar JSON file.

If workflow packages already declare document type media/extensions and root
capabilities, Fala can infer those routes from `--pipeline-dir`:

```bash
uv run fala \
  --pipeline-dir ./pipelines \
  discover-documents \
  --input-dir ./incoming \
  --auto-route \
  --run-id run_mixed_batch \
  > ./run-input.json
```

Auto-routing fails on ambiguous matches, for example when two pipelines accept
the same PDF document type. Use `--route` or source-list `pipeline_id` /
`document_type` columns to disambiguate.
The same routing is available in core service/API/client flows: send
`auto_route: true` or explicit `routes` to validate, plan, create, or append
run documents. `POST /api/process-runtime/runs/route` returns the routed
`RuntimeRunInput`, a separate `route_report` explaining which route matched each
document, rejected route candidates with reason codes, plus the same validation
preview before anything is persisted. Each document report keeps the applied
`routes` list and also includes `candidates`, `candidate_count`,
`matched_candidate_count`, and `unmatched_reasons` so operators can see why a
PDF, email, image, generated asset request, or other document did not enter the
expected pipeline. Routing does not write audit details into document `metadata`,
so strict document schemas stay unchanged. API-created routed runs also store
that report in
`run.metadata.process_runtime.run_provenance`; append-document requests return
their own `route_report` and persist routed append reports under
`run_provenance.append_batches`.
The web `/runs/new` form exposes the same path with `Auto route`, so operators
can launch mixed document batches without selecting one pipeline for the whole
run. Web-created auto-routed runs persist the same `route_report` in run
provenance and show it on the run detail page.

Edit `contracts/` when iterating on schemas, streams, or process policy, then
sync those editable files back into runtime YAML:

```bash
uv run fala sync-contracts \
  --package-yaml ./pipelines/media/process-runtime-package.yaml \
  --pipeline-yaml ./pipelines/media/media_flow.yaml \
  --contract-dir ./pipelines/media/contracts
```

Workers can claim by capability instead of hard-coding one process id:

```bash
uv run process-runtime-worker \
  --base-url http://localhost:8000 \
  --run-id run_1 \
  --pipeline document_processing \
  --worker-id ocr_worker \
  --adapter-kind queue \
  --capability extract_text \
  --gpu-count 1 \
  --memory-mb 16384 \
  --resource-label cuda \
  --command python workers/extract_text.py
```

Runtime events are also available as SSE:

```text
GET /api/runs/{run_id}/process-runtime/events/stream
```

Use `process_id` and `operation_type` query parameters to follow one program or
one class of work. Event payloads include `operation_type` when the process has
one through its capability or step declaration.

Steps can also emit chunked stream data. This is for long or incremental work:
pages from OCR, email parts, image tiles, video frames, generated scenes, or any
other document-derived sequence.

Typed packages can declare stream contracts on capabilities:

```yaml
capabilities:
  - id: extract_text
    emits_streams:
      - stream: pages
        kinds: [page]
        consumers: [enrich]
        emits_artifact_kinds: [page_text]
        max_buffered_chunks: 128
        value_schema:
          type: object
          required: [text]
          properties:
            text:
              type: string
```

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  stream-append \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --document-id contract.pdf \
  --process-id extract \
  --stream-id pages \
  --kind page \
  --value text="page text"
```

Read from a cursor and store consumer progress:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  stream-list \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --document-id contract.pdf \
  --process-id extract \
  --stream-id pages \
  --after-sequence 10

uv run fala \
  --pipeline-dir examples/pipelines \
  stream-checkpoint \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --document-id contract.pdf \
  --process-id extract \
  --stream-id pages \
  --consumer-id enrich \
  --sequence 11

uv run fala \
  --pipeline-dir examples/pipelines \
  stream-lag \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --operation-type extract \
  --stream-id pages \
  --consumer-id enrich
```

API:

```text
GET  /api/runs/{run_id}/process-runtime/stream-lag
POST /api/runs/{run_id}/process-runtime/{document_id}/processes/{process_id}/streams/{stream_id}/chunks
GET  /api/runs/{run_id}/process-runtime/{document_id}/processes/{process_id}/streams/{stream_id}/chunks
GET  /api/runs/{run_id}/process-runtime/{document_id}/processes/{process_id}/streams/{stream_id}/chunks/{chunk_id}/artifacts/{artifact_id}/download
PUT  /api/runs/{run_id}/process-runtime/{document_id}/processes/{process_id}/streams/{stream_id}/checkpoints/{consumer_id}
GET  /api/runs/{run_id}/process-runtime/{document_id}/processes/{process_id}/streams/{stream_id}/checkpoints/{consumer_id}
```

Runtime state and the web panel show stream summaries per step: chunk count,
latest sequence, artifact count, value keys, kind counts, declared consumers,
checkpoint consumers, checkpoint lag, and step `operation_type`. The stream-lag
read model and web panel list lag by consumer, stream, document, process, and
operation type, including declared consumers that have not checkpointed yet and
`max_buffered_chunks` breaches. It supports `operation_type` beside process,
capability, adapter, stream, and consumer filters. If a typed stream declares
`max_buffered_chunks`, Fala blocks new chunks once buffered data over the
slowest checkpoint exceeds that limit. `package-doctor` warns when a
backpressure-limited stream has no declared consumers.

External workers can use the client cursor helper instead of manually reading
and writing checkpoint endpoints:

```python
batch = await client.read_stream_batch(
    run_id=ctx["run_id"],
    document_id=ctx["document_id"],
    process_id="extract",
    stream_id="pages",
    consumer_id="chunk",
    limit=100,
)
for chunk in batch.chunks:
    await process_page(chunk.values)
await client.commit_stream_batch(batch)
```

Runtime queue metrics expose bottlenecks and capacity:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  queue-metrics \
  --db /tmp/fala-example.db \
  --run-id run_batch_1
```

API:

```text
GET /api/runs/{run_id}/process-runtime/metrics
```

The metrics group process instances by pipeline/process id and report queued,
running, failed, retry backoff, worker coverage, missing workers, capacity,
saturation, resource-blocked work, required resources, pool usage, and oldest
queued/running documents.

Metrics also include `worker_demands`: per-process autoscaling hints with
claimable queue size, target worker count, healthy worker count, worker deficit,
and declared package worker ids. Fala does not assume Kubernetes, Celery, or a
cloud queue. It emits a generic demand signal that any supervisor/autoscaler can
consume.

Run health aggregates those signals into one operator status:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  health \
  --db /tmp/fala-example.db \
  --run-id run_batch_1
```

API:

```text
GET /api/runs/{run_id}/process-runtime/health
```

Health returns `healthy`, `warning`, or `critical` plus issue records for missing
workers, failed processes, retry backoff, saturated capacity, stream
backpressure, and unhealthy workers.

Worker health exposes live worker heartbeats:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  worker-health \
  --db /tmp/fala-example.db \
  --run-id run_batch_1
```

API:

```text
GET /api/runs/{run_id}/process-runtime/workers
POST /api/runs/{run_id}/process-runtime/workers/{worker_id}/heartbeat
```

`process-runtime-worker` sends heartbeats automatically. Health is derived from
last seen time plus worker status, so operators can distinguish empty queues from
missing workers. Queue metrics mark queued process instances as
`missing_worker` when no healthy heartbeat matches the pipeline/process adapter,
declared capability, and resource requirements.

Process trace exposes attempt history:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  trace \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --process-id ingest \
  --operation-type ingest
```

API:

```text
GET /api/runs/{run_id}/process-runtime/trace?process_id=ingest&operation_type=ingest
```

Trace groups process events into attempts, including worker id, status, event
types, retry scheduling data, timestamps, and duration when an attempt has a
terminal event. Trace and embedded event payloads carry `operation_type`, so the
same history can be sliced by generic work class across many concrete processes.

Operators can control process cards, dead-letter replay, and stuck-work actions
from the web panel or API with `retry`,
`skip`, `fail`, and `cancel`.
Manual `retry`, dead-letter replay, and run `resume` check run provenance first.
If stored pipeline contracts drifted from the current registry, Fala blocks the
mutation unless the caller sets `allow_contract_drift` or CLI
`--allow-contract-drift`.

Runs can be paused, resumed, or cancelled:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  control-run \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --action pause
```

API:

```text
POST /api/process-runtime/runs/{run_id}/actions
```

Paused runs keep current state and block new claims until resumed. Running
workers can finish or renew their current claims. Cancelling a run marks all
unfinished process instances as `cancelled`, clears active claims, blocks new
claims, and rejects late process output or stream writes.

Operator mutations are recorded in a separate audit log. API callers can set
`X-Fala-Actor` and `X-Fala-Source`; web panel actions default to
`web-panel`/`web`, and worker writes default to `worker:{worker_id}` when a
worker id is supplied.

For local development, the API and web panel are open by default. Set
`FALA_API_KEYS` to enable the built-in API-key policy for both API and panel.
Roles are `viewer`, `worker`, `operator`, and `admin`:

```bash
export FALA_API_KEYS='viewer-secret:viewer,worker-secret:worker,operator-secret:operator,admin-secret:admin'

curl -H "Authorization: Bearer viewer-secret" \
  http://localhost:8000/api/process-runtime/runs

FALA_API_KEY=worker-secret process-runtime-worker \
  --base-url http://localhost:8000 \
  --run-id run_batch_1 \
  --pipeline basic_enrichment \
  --worker-id worker_1 \
  --adapter-kind queue \
  --command python steps/enrich.py
```

`viewer` can read. `worker` can claim/write process state. `operator` can create
runs, append documents, pause/resume/cancel, and complete manual work. `admin`
can also run destructive artifact GC and retention deletes. JSON key specs are
also supported when keys need stable actors or tenant ids. A key with
`tenant_id` stamps new runs and cannot access runs stamped with another tenant.

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  audit-log \
  --db /tmp/fala-example.db \
  --run-id run_batch_1
```

```text
GET /api/process-runtime/audit?run_id={run_id}
```

Output artifacts written through the runtime API are materialized into a
content-addressed file store (`fala-artifact://sha256/...`). Configure the root
with `FALA_ARTIFACT_STORE_ROOT`; default is `.flow-runs/artifact-store`.
`FALA_ARTIFACT_STORE` can select a backend target. Plain paths and `file:/...`
use the local content-addressed store; `memory://name` is useful for tests,
previews, and embedded bootstrap flows; `s3://bucket/prefix` uses S3-compatible
object storage when `fala[s3]` is installed and AWS/boto3 configuration is
available. Hosts can also inject an `ArtifactStore` implementation into
`RuntimeService`.
Worker SDK helpers resolve `fala-artifact://sha256/...` through the same
`FALA_ARTIFACT_STORE` target. For remote stores, blobs are downloaded into
`FALA_ARTIFACT_CACHE_ROOT` or a temp cache before the worker reads them.
Lineage metadata keeps original input artifact refs and dependency artifact refs
visible even after output artifacts are materialized.

Prune unreferenced content-addressed blobs with a dry run first:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  artifact-gc \
  --db /tmp/fala-example.db
```

Use `--delete` to remove only orphaned blobs. Fala keeps blobs referenced by
registered document sources, document inputs, process outputs, or stream chunks.

```text
GET  /api/process-runtime/artifacts/gc
POST /api/process-runtime/artifacts/gc
```

Prune old runtime history with an explicit cutoff. Dry run is default, and the
default statuses are terminal runs only: `completed`, `failed`, and `cancelled`.

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  run-retention \
  --db /tmp/fala-example.db \
  --older-than-days 30
```

Use `--delete` to remove selected run state from the runtime DB. This does not
delete artifact blobs; run `artifact-gc --delete` afterwards to reclaim blobs
that became orphaned.

```text
GET  /api/process-runtime/runs/retention
POST /api/process-runtime/runs/retention
```

Package workers can be supervised as one runtime group:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  supervise-workers \
  --base-url http://localhost:8000 \
  --run-id run_1 \
  --package-id basic_examples
```

The same package worker declarations can bootstrap a full local stack:

```bash
uv run fala --pipeline-dir examples/pipelines \
  deployment \
  --format docker-compose \
  --run-id run_1 \
  --package-id basic_examples \
  --image registry.example.com/fala:latest \
  --worker-image registry.example.com/fala-worker:latest \
  --with-postgres \
  | jq -r .manifest
```

This manifest contains the bundled API/web panel, optional Postgres, a shared
artifact volume, a pipeline bind mount for local Compose, and generated package
workers pointed at the internal control-plane URL. Use `--format kubernetes` for
Deployment/Service/PVC YAML.

If the control plane already exists, render only worker manifests:

```bash
uv run fala --pipeline-dir examples/pipelines \
  worker-deployment \
  --base-url http://runtime.default.svc.cluster.local \
  --run-id run_1 \
  --package-id basic_examples \
  --format docker-compose \
  --image registry.example.com/fala-worker:latest \
  --container-pipeline-dir /app/pipelines \
  | jq -r .manifest
```

Use `--container-pipeline-dir` when the package directory is mounted at a
different path inside the worker image. Fala rewrites `--pipeline-dir` and maps
worker `cwd` values under the host pipeline directory into that container path.
For Docker Compose, Fala also adds a read-only bind mount from the host
pipeline directory to `--container-pipeline-dir`. Use `--no-mount-pipeline-dir`
when the image already contains the package tree. Use `--container-workdir` to
override the generated worker working directory.

Runtime demand can be scraped as Prometheus text and used to bootstrap KEDA:

```bash
uv run fala --pipeline-dir examples/pipelines \
  metrics-prometheus \
  --db runtime.db \
  --run-id run_1 \
  | jq -r .metrics

uv run fala --pipeline-dir examples/pipelines \
  worker-autoscaling \
  --base-url http://runtime.default.svc.cluster.local \
  --run-id run_1 \
  --package-id basic_examples \
  --prometheus-server http://prometheus.monitoring.svc.cluster.local:9090 \
  | jq -r .manifest
```

For real brokers, use the queue bridge contract. Fala claims work, emits
`QueueWorkEnvelope`, any broker moves it, a worker emits
`QueueResultEnvelope`, then Fala applies the result back to the control plane.
JSONL files are the simplest bridge:

```bash
uv run fala queue-export-claims \
  --base-url http://localhost:8000 \
  --run-id run_1 \
  --pipeline basic_enrichment \
  --worker-id bridge-publisher \
  --capability enrich_document \
  --max-claims 10 \
  --work-file work.jsonl

uv run fala queue-run-work \
  --work-file work.jsonl \
  --result-file results.jsonl \
  --command python steps/enrich.py

uv run fala queue-apply-results \
  --base-url http://localhost:8000 \
  --result-file results.jsonl
```

For local durable broker semantics, use SQLite leases/results. Prefer the
generic `--queue-broker` target; `--queue-db` remains a compatibility alias for
plain SQLite paths:

```bash
uv run fala queue-export-claims \
  --base-url http://localhost:8000 \
  --run-id run_1 \
  --pipeline basic_enrichment \
  --worker-id bridge-publisher \
  --capability enrich_document \
  --unassigned-claim \
  --max-claims 10 \
  --queue-broker .fala/queue.sqlite

uv run fala queue-run-work \
  --queue-broker .fala/queue.sqlite \
  --queue documents.enrich \
  --worker-id enrich-worker-1 \
  --max-claims 10 \
  --max-deliveries 5 \
  --renew-claim \
  --base-url http://localhost:8000 \
  --command python steps/enrich.py

uv run fala queue-apply-results \
  --base-url http://localhost:8000 \
  --queue-broker .fala/queue.sqlite
```

`--renew-claim` keeps the original control-plane claim alive while a broker
worker command runs. Without it, a long command can outlive the exported claim
lease and become eligible for duplicate work.
Use `--unassigned-claim` when one process exports work and another process owns
execution. Then `queue-run-work --worker-id ... --renew-claim` assigns the real
broker worker before renew/output writes.
`--max-deliveries` moves poison SQLite work to `dead_letter` after repeated
delivery attempts, instead of leasing the same broken item forever.
Inspect and requeue poison rows after fixing the worker/package:

```bash
uv run fala queue-list-work \
  --queue-broker .fala/queue.sqlite \
  --state dead_letter \
  --include-payload

uv run fala queue-requeue-work \
  --queue-broker .fala/queue.sqlite \
  --work-id work_123
```

`queue-requeue-work` resets `delivery_count` by default. Use
`--keep-delivery-count` only when the next lease should keep prior attempts.
The web panel can inspect and requeue the same broker rows when started with
`FALA_QUEUE_BROKER=.fala/queue.sqlite`, `FALA_QUEUE_DB=.fala/queue.sqlite`,
`FALA_QUEUE_BROKER=redis://localhost/0`, or `create_runtime_web_app(queue_db=...)`;
open `/queue` and use the Broker queue section. `fala serve --queue-broker ...`
and `fala deployment --queue-broker ...` wire the same target into local and
generated control-plane deployments.

Failed broker results go through the same retry policy as direct workers:
retryable `error_kind` values reschedule work, terminal kinds fail it, and
`queue-apply-results` returns the process action from Fala.

`memory://name` is available for embedded tests and previews where all actors
share one Python process. It is not durable and does not coordinate separate
processes.

`redis://...` and `rediss://...` are available as a shared broker backend when
`fala[redis]` is installed. Add `?prefix=name` to isolate multiple Fala
deployments in one Redis database. SQLite remains the local durable reference;
Redis is the reference shared broker for multi-process and multi-worker
deployments.

Kafka, SQS, Pub/Sub, NATS, or a custom transport should implement
`QueueBrokerTransport` and carry those JSON envelopes plus lease/ack/result
state. The runtime still owns claims, leases, retry policy, audit, artifacts,
lineage, and run state.
