# Fala Runtime

Fala is a control layer for document workflow execution. It does not transform
documents itself. Step programs do that.

## Boundary

Framework owns:

- workflow package loading
- graph validation
- run and document registry
- document type, artifact kind, and capability manifests
- process status state
- queue claims and leases
- retry and timeout policy
- event log
- operator audit log
- stream chunks, checkpoints, and lag read models
- artifact refs
- content-addressed artifact storage
- combineLatest projections
- HTTP client, worker runner, CLI, optional FastAPI router

Step program owns:

- parsing
- enrichment
- normalization
- calls to models or external systems
- output values, artifacts, and optional `stream_chunks`

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
- `manual`: operator gate; workers never claim it

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
document_types:
  - id: generic_document
    media_types: ["application/octet-stream"]
    extensions: [".pdf", ".txt", ".eml"]
    value_schema:
      type: object
      properties:
        source:
          type: string
      additionalProperties: true
    metadata_schema:
      type: object
      properties:
        case_id:
          type: string
      additionalProperties: true
  - id: pdf_page
    media_types: ["application/pdf"]
    extensions: [".pdf"]
document_relations:
  - id: page
    source_document_types: [generic_document]
    target_document_types: [pdf_page]
operation_types:
  - id: extract
    category: extraction
artifact_kinds:
  - id: extracted_text
    media_types: ["text/plain"]
    extensions: [".txt"]
    metadata_schema:
      type: object
      properties:
        page_count:
          type: integer
      additionalProperties: true
capabilities:
  - id: extract_text
    operation_type: extract
    accepts_document_types: [generic_document]
    emits_document_types: [pdf_page]
    emits_artifact_kinds: [extracted_text]
    config_schema:
      type: object
      properties:
        model:
          type: string
      additionalProperties: false
    output_schema:
      type: object
      required: [text]
      properties:
        text:
          type: string
    emits_streams:
      - stream: pages
        kinds: [page]
        consumers: [normalize]
        emits_artifact_kinds: [extracted_text]
        value_schema:
          type: object
          required: [text]
          properties:
            text:
              type: string
        metadata_schema:
          type: object
          properties:
            page_number:
              type: integer
secrets:
  - id: openai_api_key
    env_var: OPENAI_API_KEY
    kubernetes_secret_name: fala-openai
    kubernetes_secret_key: api-key
pipelines:
  - basic_enrichment.yaml
workers:
  - id: enrich_worker
    capabilities: [extract_text]
    pipeline: basic_enrichment
    process: enrich
    secrets: [openai_api_key]
    sandbox:
      run_as_non_root: true
      read_only_root_filesystem: true
      allow_privilege_escalation: false
      drop_capabilities: [ALL]
    resources:
      memory_mb: 2048
      labels: [cpu]
    command: ["python", "steps/enrich.py"]
    cwd: "."
```

Pipeline steps can bind to a capability:

```yaml
steps:
  - id: extract
    capability: extract_text
    adapter:
      kind: queue
      queue: documents.extract
```

Workers can claim work by capability. If a claim request includes
`capabilities`, Fala only returns queued steps whose `capability` matches one of
those ids. Requests without capabilities keep the legacy behavior and match by
pipeline, process id, and adapter kind only.
Capabilities can also bind to `operation_type`. Runtime state summaries,
process records, queue metrics, dead-letter, stuck-work, stream-lag, and
run-results expose that operation type, so operators can supervise broad work
classes such as `extract`, `generate`, `review`, or `export` without knowing
local step ids.

Steps can define generic execution policy:

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

Workers claim higher `priority` queued steps first. `max_concurrency` caps
concurrent claims for the same process within one run. This keeps expensive
steps such as OCR, LLM calls, image rendering, or video processing under control
without making Fala specific to any document type. `retry.delay_seconds` keeps
failed or expired attempts in `waiting` backoff before another worker can claim
the step. Workers can report `error_kind`; `terminal_error_kinds` fail without a
retry, while a non-empty `retry_error_kinds` allowlist retries only matching
classified failures.

`sla` is operator policy for stuck-work detection. `stuck-work` uses per-step
thresholds first, then falls back to CLI/API query defaults. This keeps slow
rendering steps and fast metadata steps in one generic run without hard-coded
process names.

`resources` is a generic scheduler contract. A step can require `cpu_cores`,
`memory_mb`, `disk_mb`, `gpu_count`, labels, and named numeric `units`. A worker
declares its available resources in a heartbeat or package worker manifest. Fala
only returns a queued step to workers whose resources satisfy the step
requirements. Missing-worker metrics use the same match, so a queued GPU step is
reported as uncovered when only CPU workers are alive.

Manual gates are first-class process steps. A ready manual step moves to
`waiting` and emits `process.manual_required`. It does not create worker demand.
Complete it by writing a regular `ProcessOutput`; downstream steps then schedule
normally:

```yaml
steps:
  - id: review
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

Conditional steps use `when` for generic routing inside a static DAG. The
scheduler supports exact `document_types`, wildcard-capable `media_types`, and
exact document `metadata` / initial `values` key matches. A non-matching step is
`skipped`; any step that depends on a skipped output is skipped too.

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

Dynamic fan-out uses `ProcessOutput.spawn_documents`. A split/chunk/extract step
can emit child documents such as PDF pages, email attachments, normalized JSON
records, image frames, or video segments. Fala stamps parent lineage onto each
child, stores it in the same run, initializes its input, and schedules the child
pipeline. Workers can set `pipeline_id` directly, but they can also emit only
the child `document_type`, media type, source URI, values, and artifacts. Fala
then auto-routes the child from workflow package document contracts and falls
back to the parent pipeline when no contract matches. The stored process output
keeps `metadata.process_runtime.spawn_route_report`, so operators can see the
auto-route or fallback decision. Typed packages can declare allowed child
outputs with capability `emits_document_types`; Fala validates spawned documents
against that contract and declared `document_relations` after routing.

Parent processes can fan in after child work by declaring `wait_for_children`.
The process first follows normal same-document `needs`, then waits for matching
child documents to reach the required document statuses:

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
schedule it as another work item:

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
  "values": {"attachment_count": 1},
  "spawn_documents": [
    {
      "document_id": "mail.eml#attachment-1",
      "document_type": "pdf_document",
      "media_type": "application/pdf",
      "source_uri": "file:///tmp/attachment-1.pdf"
    }
  ]
}
```

Operators can complete a manual split with the full output JSON:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  complete-process \
  --db /tmp/fala-example.db \
  --pipeline email_flow \
  --run-id run_batch_1 \
  --document-id mail.eml \
  --process-id split_attachments \
  --output-file split-output.json
```

The document lineage read model exposes the resulting graph:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  document-lineage \
  --db /tmp/fala-example.db \
  --run-id run_batch_1
```

API:

```text
GET /api/runs/{run_id}/process-runtime/document-lineage
```

Use `run-results` to export compact output rows across the run. It is the
generic handoff for reports, search indexes, vectorization, downstream reduce
jobs, or another system:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  run-results \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --pipeline attachment_flow \
  --process-id export \
  --jsonl
```

Filters:

- `pipeline_id`
- `process_id`
- `document_id`
- `document_type`
- `operation_type`

API:

```text
GET /api/runs/{run_id}/process-runtime/results
```

Typed document products are also exposed as a first-class list:

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

Declare `reduces` in a pipeline when the run should expose a named aggregate
over its result rows:

```yaml
reduces:
  - id: exported_documents
    process_id: export
    mode: collect_values
```

Available modes:

- `collect_values`: collect output `values`; optional `value_key` narrows to one
  value
- `collect_outputs`: collect full output payloads
- `count`: count rows by process, document type, and pipeline

Compute declared reductions:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  run-reductions \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --pipeline attachment_flow
```

API:

```text
GET /api/runs/{run_id}/process-runtime/reductions
```

The web panel renders reductions at `/runs/{run_id}/process-runtime/reductions`
and compact result rows at `/runs/{run_id}/process-runtime/results`.

Runs can define pool quotas in `config.resource_pools`. `resource_pool` on a
step selects which pool it consumes. Missing `resource_pool` means `default`.

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

When pool quota is saturated, Fala does not issue a new claim for that pool even
if a matching worker is healthy. Queue metrics expose `resource_blocked_count`,
per-pool `used`, `remaining`, running count, queued count, and saturation.

Worker CLI resource flags:

```bash
uv run process-runtime-worker \
  --base-url http://localhost:8000 \
  --run-id run_1 \
  --pipeline document_processing \
  --worker-id ocr_gpu_1 \
  --adapter-kind queue \
  --capability extract_text \
  --gpu-count 1 \
  --memory-mb 16384 \
  --resource-label cuda \
  --resource-unit ocr_slots=2 \
  --command python workers/ocr.py
```

Fala validates package wiring at load time: step capability references must
exist, step `config` must match capability `config_schema`, root steps in typed
packages must accept document types, and dependent steps with artifact inputs
must accept at least one artifact kind emitted by their needs. At runtime, batch
document `values` and `metadata` must match the document type schemas, and output
artifacts written through the API must use artifact kinds declared by the package
and emitted by the process capability. Spawned child documents must use package
document types and, when declared, the process capability's
`emits_document_types`. Output documents use the same emitted document type
contract but do not schedule more work; they are for produced documents such as
redacted PDFs, translated DOCX files, rendered videos, or generated scripts.
Stream chunks are checked against capability
`emits_streams` when declared: `stream_id`, optional `kind`, `values`,
`metadata`, and emitted artifact kinds must match the stream contract.
Artifact metadata, media type, and extension are checked against the artifact
kind contract when declared. When an artifact kind declares `value_schema`, the
artifact URI must resolve to UTF-8 JSON that matches that schema. Output
`values` are validated against the process capability `output_schema` when
present. Stored outputs include
`metadata.process_runtime.lineage`, which records input artifact summaries,
dependency output summaries, value keys, process id, document id, capability,
attempt, and worker id when known.

Preflight one pipeline's typed contract before creating runs:

```bash
uv run fala --pipeline-dir examples/pipelines contract basic_enrichment
```

API:

```text
GET /api/process-runtime/pipelines/{pipeline_id}/contract
```

The contract includes accepted document types, artifact kind definitions,
per-step capability schemas, emitted artifact kinds, execution policy, combine
projections, and declared workers for the pipeline.

Export a versioned package release index:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  package-index \
  --output package-index.json
```

API:

```text
GET /api/process-runtime/packages/index
GET /api/process-runtime/packages/{package_id}/release
```

The index includes package id/version, manifest file SHA-256, canonical package
model SHA-256, package contract SHA-256, pipeline model SHA-256, pipeline
contract SHA-256, source paths, document type ids, artifact kind ids,
capability ids, worker ids, and pipeline ids. Use it as a release lock for
deployments, cache keys, rollout checks, or audit. The digest is independent of
which worker happens to execute the work.

Check whether a package is ready to bootstrap a concrete document workflow:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  package-doctor
```

The doctor report checks routeable document types, pipeline DAG artifact
contracts, queue worker coverage, package worker command availability, sample
run input presence, runtime validation for sample run/source-list manifests,
source-list sample source files, unused capabilities/artifact kinds/secrets, and
blocking package errors.
This keeps project bootstrap separate from release digests: `package-index`
proves identity, `package-doctor` proves operational readiness. The web panel
shows the same readiness summary on `/process-runtime/pipelines`. For generated
multi-package workspaces, `fala-project.yaml` adds a root-level project check
that covers workspace files, package readiness, and mixed source-list routing;
the panel shows it on `/project` and the operator bootstrap view on
`/project/bootstrap`. API:

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

Preflight concrete run input before creating a run:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  validate-run \
  --run-input examples/pipelines/basic/run-input.example.yaml

uv run fala \
  --pipeline-dir examples/pipelines \
  validate-run \
  --pipeline basic_enrichment \
  --existing-run resume \
  --existing-document reuse \
  --document-type generic_document \
  --file ./documents/example.txt
```

API:

```text
POST /api/process-runtime/runs/validate
```

This is side-effect free. It checks the submitted documents against typed
package contracts and returns per-document summaries plus every matched pipeline
contract.

The response includes `document_summary`: counts by pipeline id, document type,
media type, source URI scheme, artifact kind, value keys, metadata keys, source
URI presence, source SHA-256 presence, and missing type/media counts. This keeps
large batch manifests inspectable before any runtime state is written.

Use `plan-run` after validation when an operator needs the dry execution shape
before creating state. It returns per-document queued and waiting steps, process
group totals, declared package workers, initial and eventual worker targets, and
resource pool requests. It does not create a run or documents.

When a batch run is created, Fala stores a provenance snapshot in
`run.metadata.process_runtime.run_provenance`. It contains the canonical run input
SHA-256, pipeline contract SHA-256, plan SHA-256, document summary, compact plan
without per-document rows, and the pipeline contracts used for that run. This
keeps later debugging tied to the contract that existed when the run started.
Later document appends add compact `append_batches` entries with input SHA-256,
document summary, schedule summary, and route report/hash when routing was used.

Fetch it directly when an operator or external system needs the stored snapshot.
The response includes `contract_drift`, which compares those stored pipeline
contracts against the current registry. `status: current` means replay/resume
uses the same contract; `status: drifted` lists changed or missing pipelines.
The web provenance panel shows the same drift table.

API:

```text
POST /api/process-runtime/runs/plan
GET  /api/process-runtime/runs/{run_id}/provenance
```

Use `inspect-run-input` when the manifest may not pass runtime validation yet.
It reads raw JSON/YAML and reports duplicate `document_id` as an error plus
duplicate `source_sha256` and duplicate `source_uri` as warnings. It also returns
the same aggregate `document_summary`, so operators can fix manifest problems
before `validate-run` or `create-run`.

Preflight can read a full run manifest:

```bash
uv run fala discover-documents \
  --input-dir ./incoming \
  --pipeline basic_enrichment \
  --run-id run_batch_1 \
  --document-type generic_document \
  --content-hash \
  > ./run-input.json

uv run fala discover-documents \
  --source-list ./sources.csv \
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
```

`--run-input` accepts JSON or YAML matching `RuntimeRunInput`. CLI flags override
scalar fields (`--run-id`, `--pipeline`, `--title`, conflict policy) and append
extra `--file` or `--document` entries. `--resource-pool` merges into
`config.resource_pools`.

`discover-documents` scans local files and writes a reusable `RuntimeRunInput`
manifest. For files found under `--input-dir`, `document_id` is the relative path
with `/` separators. It fills `source_uri`, inferred `media_type`, title, and
source metadata (`source_path`, `source_size`, `source_mtime`). Use `--include`
and `--exclude` fnmatch patterns to shape large imports.

Use `--content-hash` to add `metadata.source_sha256` for local files. Use
`--document-id-mode sha256` when repeated imports should identify documents by
content fingerprint instead of path. Source lists can provide `source_sha256`,
`sha256`, or `source_hash` for remote documents.

For heterogeneous intake, add one or more route manifests. First matching rule
fills missing per-document `pipeline_id`, `document_type`, `media_type`,
`values`, and `metadata`, so a folder or source-list can become one mixed
`RuntimeRunInput` without a run-level pipeline:

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

Route matches support `extensions`, wildcard `media_types`, `source_uri_globs`
or `source_globs`, `document_id_globs`, `title_globs`, `document_types`, exact
`values`, and exact `metadata`. Source-list rows can also set `pipeline_id` or
`pipeline` directly. `--route-report` writes sidecar diagnostics with applied
routes, rejected candidates, and reason codes while stdout remains the
`RuntimeRunInput` manifest.

When workflow packages already declare document type media/extensions and root
capabilities, `--auto-route` builds route candidates from the registry:

```bash
uv run fala \
  --pipeline-dir ./pipelines \
  discover-documents \
  --input-dir ./incoming \
  --auto-route \
  --run-id run_mixed_batch \
  > ./run-input.json
```

Auto-routing uses package `document_types` plus root-step capabilities that
`accepts_document_types`. It fills missing per-document `pipeline_id` and
`document_type`. If more than one pipeline/document-type candidate matches a
document, Fala fails the discovery command and asks for explicit `--route` or
source-list `pipeline_id` / `document_type`, instead of silently choosing the
wrong workflow.

Routing is not CLI-only. The core service exposes
`route_runtime_document_inputs`, the Python client exposes `route_run`, and the
API accepts `auto_route: true` or explicit `routes` on validate, plan, create,
and append-document requests. Use this endpoint when another UI or ingestion
service needs to preview the routed manifest before persistence:

```text
POST /api/process-runtime/runs/route
```

The response includes `run_input` with routed documents, `route_report` with the
route id/kind/evidence for each document, rejected route candidates with reason
codes, plus `preview` with the same contract validation summary returned by
`/process-runtime/runs/validate`. Each document report includes applied `routes`
and diagnostic `candidates`; a rejected candidate has `match: false` and
`reasons` such as `extension_mismatch`, `media_type_mismatch`, or
`document_type_missing`. Route reports stay outside document `metadata`, so
strict document schemas do not need routing audit fields. When an API create-run
request uses auto or explicit routing, the same report is persisted under
`run.metadata.process_runtime.run_provenance.route_report` with a
`route_report_sha256`. Append-document requests return their own `route_report`
and include routing details in operator audit data. Routed appends are also
recorded under `run_provenance.append_batches`.
The web `/runs/new` form exposes the same behavior through `Auto route`, so
operators can paste URIs or upload files for mixed batches without selecting a
single run-level pipeline. Web-created auto-routed runs persist the same
`route_report` and expose it in the run provenance panel.

`--source-list` accepts CSV or TSV source manifests. Supported columns:
`document_id`, `title`, `source_uri`, `path` or `source_path`, `pipeline_id` or
`pipeline`, `document_type`, `media_type`, `source_sha256`/`sha256`/
`source_hash`, plus dynamic `value.NAME` and `metadata.NAME` columns. `path` is
resolved relative to the source-list file. Each row becomes one
`RuntimeDocumentInput`.

Inspect operational queue metrics for a run:

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

Metrics are grouped by pipeline/process id and include queued/running/waiting
counts, retry backoff count, next retry time, worker coverage, missing worker
count, capacity remaining, saturation, oldest queued document, oldest running
document, resource-blocked work, required resources, resource pool usage, and
last event timestamp. This is the operator view for spotting which generic
workflow step is the current bottleneck.

Metrics also include `worker_demands`. Each demand record is a platform-neutral
autoscaling signal: claimable queued work, target worker count, healthy matching
workers, worker deficit, process resources, resource pool, and declared package
worker ids. A local supervisor, Kubernetes controller, batch scheduler, or queue
backend can consume this without Fala owning infrastructure.

Inspect aggregate run health:

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

Health is a read model over queue metrics and worker heartbeats. It returns
`healthy`, `warning`, or `critical`, plus issue records for missing workers,
failed processes, retry backoff, saturated capacity, stream backpressure, and
unhealthy workers.

Inspect worker health:

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

`process-runtime-worker` sends heartbeats before polling, while working, during
claim renewal, and after completion/error. Health is derived from `last_seen_at`,
status, and `stale_after_seconds`. Queue metrics use the same routing filters as
claims and mark queued process instances as missing workers when no healthy
worker matches the pipeline/process adapter, capability, and resource
requirements. This keeps queue state separate from fleet state: a run can have
no claimable work because it is blocked, or because no healthy worker exists for
a capability or resource profile.

Inspect attempt history:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  trace \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --document-id invoice_1.pdf \
  --process-id extract \
  --operation-type extract
```

API:

```text
GET /api/runs/{run_id}/process-runtime/trace?document_id=invoice_1.pdf&process_id=extract&operation_type=extract
```

Trace is a read model over the event log. It groups events by attempt number and
returns worker id, final attempt status, event types, timestamps, duration, and
full event payloads. Trace and embedded process events include `operation_type`
when a capability or step declares one, so operators can inspect retries by
generic work class instead of only by process id. This is the operator/debug view
for understanding what happened across retries and previous process executions.

## Project Scaffold

Use `scaffold` to start a new package from the generic contract:

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

Use a named blueprint when the workflow shape is common:

```bash
uv run fala scaffold-blueprints

uv run fala scaffold-blueprints --query "translated document"

uv run fala scaffold-blueprints --blueprint generative_media

uv run fala scaffold \
  --output-dir ./pipelines/media \
  --package-id media_processing \
  --pipeline-id media_flow \
  --blueprint generative_media \
  --adapter-kind queue
```

For a heterogeneous project with several document/work types, initialize a
workspace from multiple blueprints:

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
`pipelines/<blueprint>/...` packages plus a root Makefile and `fala-project.yaml`
manifest for whole-workspace `validate`, `doctor`, `project-doctor`, package
bootstrap, and web/API startup. The manifest also includes editable
`alerts.rules` over operations metrics and `lifecycle.run_retention` for
project-scoped cleanup planning. It writes a root `source-list.example.csv` that
references one sample document from each package and
`document-routes.example.yaml` as editable intake policy. `make project-doctor`
checks root samples, package readiness, and mixed routing from `fala-project.yaml`.
`make db-doctor` checks the runtime database target and creates/repairs the local
schema when `DB` points at an empty SQLite file or Postgres DSN; the report
includes runtime schema version and applied migrations. `make project-check`
writes one aggregate bootstrap report over project readiness,
spec generation, secret inventory, DB readiness, and optional bundle verification.
`make project-smoke` creates the mixed sample run and executes local subprocess
steps plus declared queue worker commands until the run completes or the smoke
report captures the first failure. `make mixed-source-list` compiles that source
list with `--route document-routes.example.yaml` and `--auto-route` fallback into
one mixed run input, and `make create-mixed` uses `fala create-project-run` to
create that run with project metadata. `make project-spec` writes the same project runbook to
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

```python
from fala import get_scaffold_blueprint, list_scaffold_blueprints

catalog = list_scaffold_blueprints()
translation_catalog = list_scaffold_blueprints(query="translated_document")
blueprint = get_scaffold_blueprint("generative_media")
```

Built-in blueprints:

- `document_digitalization`: ingest, extract, normalize, enrich, assemble,
  export; parent documents run ingest/extract/assemble and wait on
  `page_document` children, while page children run ingest/normalize/enrich/export
  and skip split/join steps
- `email_processing`: ingest email, parse message, extract attachments,
  classify, export; message documents can emit `email_attachment_document`
  children, and attachment children run ingest/classify/export while skipping
  MIME parse and attachment extraction
- `document_package_processing`: ingest package, inspect package, extract items,
  classify items, route items, export items and package manifest; package roots
  can emit `packaged_document` children
- `document_redaction_review`: ingest, extract text, detect sensitive spans,
  redact, review, export a `redacted_document`
- `document_translation_review`: ingest, extract text, segment, translate,
  review, assemble, export a `translated_document`
- `generative_media`: ingest brief, plan, generate assets, render, export
- `llm_document_processing`: ingest, extract text, chunk, embed, retrieve,
  generate, review, export
- `knowledge_base_ingestion`: ingest, extract text, split chunks, enrich
  metadata, embed, index
- `structured_extraction_review`: ingest, extract text, extract fields,
  validate fields, review, export
- `tabular_data_processing`: ingest CSV/XLS/XLSX/JSON table data, profile rows,
  normalize rows, validate rows, enrich records, aggregate, export

`scaffold-blueprints` returns JSON with document contracts, extra document
types, accepted/emitted document types per step, step order, DAG `needs`,
capability/artifact mappings, streams, manual gates, resource pools, and a
starter scaffold command for each preset. The web panel shows the same catalog at
`/process-runtime/blueprints`, with `query` filtering for intent-driven
selection (`redact`, `translation`, `email`, `csv`, `video`, `package item`,
operation types, document types, capabilities, streams, or resource pools). The
API exposes the same catalog at:

```text
GET /api/process-runtime/blueprints?query=translation
GET /api/process-runtime/blueprints/{blueprint_id}
```

Blueprints are bootstrap presets only. They generate normal
`process-runtime-package.yaml`, pipeline YAML, `run-input.example.yaml`,
`source-list.example.csv`, an `incoming/` sample source file,
`README.scaffold.md`, `Makefile`, `contracts/` editable schema/policy
templates, worker manifests, step programs, child document output contracts,
artifact media contracts, artifact value schemas, capability output schemas, and
common stream contracts. The generated README summarizes the contract surface,
step policy, worker mode, manual gates, and operator commands. The generated
Makefile exposes the same bootstrap path as targets: `make bootstrap`,
`make create`, `make run-local`, `make serve`, and `make worker`.
Blueprints also prefill conservative step policy such as retry, SLA, priority,
max concurrency, resource pools, child wait barriers, and manual review gates
where the workflow shape calls for them. They avoid hard worker resource
requirements so generated samples stay locally runnable; add those requirements
with `--step-policy` when needed. Blueprint step policy files are deep-merged
into defaults, so partial overrides can add hard resources or tweak retry
without discarding default SLA,
priority, resource pool, or retry backoff.
Override the document type with
`--document-type`, media types with `--document-media-type`, file extensions
with `--document-extension`, output artifact extensions with
`--artifact-extension STEP=EXT`, per-step artifact schemas with
`--artifact-value-schema STEP=PATH`, per-step process output schemas with
`--capability-output-schema STEP=PATH`, stream contracts with
`--stream-contract STEP=PATH`, process policy with `--step-policy STEP=PATH`,
and document input contracts with `--document-value-schema` /
`--document-metadata-schema` when needed. Step policy files can set manual gates,
retry, priority, resource requirements, `when` routing, config, and other
process policy fields without changing generated artifact/capability contracts.
Generated SDK step programs emit sample payloads that satisfy those schemas, so
the scaffold remains executable while business logic is still a placeholder.

Projects can define their own blueprint YAML files when the built-in presets
are only a starting point:

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

Inspect or scaffold from that file:

```bash
uv run fala scaffold-blueprints --blueprint-file ./invoice-blueprint.yaml

uv run fala scaffold \
  --output-dir ./pipelines/invoices \
  --package-id invoice_processing \
  --pipeline-id invoice_flow \
  --blueprint-file ./invoice-blueprint.yaml \
  --adapter-kind queue
```

Custom blueprint fields map to the same runtime contracts as built-in
blueprints: document type/media/extensions/value schema/metadata schema, step
ids, DAG `needs`, capabilities, artifact kinds, extra document types,
extra document relations, operation types, per-step
accepted/emitted document types, optional artifact media/extensions/value
schemas, capability output schemas, stream specs, and step policy. They are
bootstrap presets only; generated package files stay normal editable Fala
contracts. Child waits live in step policy as `wait_for_children`.

The generated `run-input.example.yaml` is a normal `RuntimeRunInput` with sample
document `values` and `metadata` from the scaffolded schemas, so it uses the
same path as production batches:

```bash
uv run fala --pipeline-dir ./pipelines/media \
  validate-run \
  --run-input ./pipelines/media/run-input.example.yaml

uv run fala --pipeline-dir ./pipelines/media \
  create-run \
  --db runtime.db \
  --run-input ./pipelines/media/run-input.example.yaml
```

For larger batches, edit `source-list.example.csv` and compile it into a
manifest with the generic source-list importer:

```bash
uv run fala discover-documents \
  --source-list ./pipelines/media/source-list.example.csv \
  --pipeline media_flow \
  --run-id run_media_batch \
  > ./run-input.json
```

Generated files include:

- `process-runtime-package.yaml` with document types, document input value
  schemas, artifact kinds, capabilities, starter JSON Schema contracts, stream
  contracts, pipelines, and queue worker manifests when requested
- `{pipeline_id}.yaml` with a linear workflow, capability refs, and a
  `workflow_result` combine
- `source-list.example.csv` with supported source-list columns, including
  `relation`, `parent_document_id`, `parent_process_id`, `value.*` columns
  inferred from document `value_schema`, and `metadata.*` columns inferred from
  document `metadata_schema`
- `incoming/{sample}` source file referenced by the sample source list, so batch
  discovery can run immediately after scaffolding
- `steps/{process_id}.py` SDK-backed starter programs that emit progress,
  values, and a JSON artifact

Edit `contracts/` while shaping a document domain, then sync those editable
schema, stream, and policy files back into runtime YAML:

```bash
uv run fala sync-contracts \
  --package-yaml ./pipelines/media/process-runtime-package.yaml \
  --pipeline-yaml ./pipelines/media/media_flow.yaml \
  --contract-dir ./pipelines/media/contracts
```

For external workers, add `--adapter-kind queue`. Generated workers can be
listed with `worker-commands` or supervised with `supervise-workers`.

## Batch Document Input

Runs can be created with many documents in one request:

```json
{
  "run_id": "run_batch_1",
  "existing_run_policy": "resume",
  "existing_document_policy": "reuse",
  "title": "Mailbox import",
  "pipeline_id": "basic_enrichment",
  "documents": [
    {
      "document_id": "contract.pdf",
      "document_type": "generic_document",
      "media_type": "application/pdf",
      "source_uri": "file:///data/contract.pdf",
      "values": {"case_id": "A"}
    },
    {
      "document_id": "email_1",
      "media_type": "message/rfc822",
      "source_uri": "s3://bucket/email_1.eml"
    }
  ]
}
```

POST it to `/api/process-runtime/runs`.

When a batch document declares `document_type`, Fala checks that the type is
declared by the workflow package and accepted by the selected pipeline root
capabilities before creating the run. Document `values` are validated against
the document type `value_schema`, and metadata is validated against
`metadata_schema`, when present. `media_type` and detectable file extension are
checked against document type `media_types` and `extensions` when they are
declared. Media types support exact matches, `type/*`, `*/*`, and
`application/octet-stream` as a generic binary fallback.

CLI equivalent:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  create-run \
  --db runtime.db \
  --pipeline basic_enrichment \
  --run-id run_batch_1 \
  --existing-run resume \
  --existing-document reuse \
  --file ./contract.pdf \
  --document email_1=s3://bucket/email_1.eml
```

Manifest equivalent:

```yaml
run_id: run_batch_1
existing_run_policy: resume
existing_document_policy: reuse
title: Mailbox import
pipeline_id: basic_enrichment
documents:
  - document_id: contract.pdf
    document_type: generic_document
    media_type: application/pdf
    source_uri: file:///data/contract.pdf
    values:
      case_id: A
  - document_id: email_1
    media_type: message/rfc822
    source_uri: s3://bucket/email_1.eml
```

Run it with:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  create-run \
  --db runtime.db \
  --run-input ./run-input.yaml
```

Fala stores each document as a first-class runtime record. The record keeps
`document_id`, optional title, type, media type, source URI, metadata, status, and
a compact lifecycle summary. Initial scheduler input still receives the same
descriptor under `values.document`, and `source_uri` is added as a source
artifact.

By default, a duplicate `run_id` or existing `document_id` fails fast. For
resumable imports, set `existing_run_policy=resume` and
`existing_document_policy=reuse`. Fala then returns the existing run, preserves
the existing document record/input, and schedules any remaining ready work
without creating duplicate documents.

Read document registry records with:

```text
GET /api/runs/{run_id}/process-runtime/documents
```

The registry endpoint returns a page envelope with `documents`, `count`,
`limit`, `offset`, `has_more`, and `filters`. Supported filters:

```text
GET /api/runs/{run_id}/process-runtime/documents?status=queued&document_type=generic_document&relation=page&limit=100&offset=0
```

CLI:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  list-documents \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --status queued \
  --limit 100
```

Process instances use the same page shape:

```text
GET /api/runs/{run_id}/process-runtime/processes?status=queued&operation_type=ingest&capability=ingest_document&limit=100&offset=0
```

Supported filters: `status`, `pipeline_id`, `document_type`,
`parent_document_id`, `document_id`, `process_id`, `capability`,
`operation_type`, `adapter_kind`, and `resource_pool`.

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

Terminal failed processes are exposed as a runtime dead-letter queue. It is a
generic read model over failed process records and their last event, including
reason, error kind, terminal reason, attempt, worker id, and suggested operator
actions:

```text
GET /api/runs/{run_id}/process-runtime/dead-letter
POST /api/runs/{run_id}/process-runtime/dead-letter/{document_id}/processes/{process_id}/replay
GET /api/runs/{run_id}/process-runtime/stuck-work
```

`replay` is a thin operator retry. It clears the failed process and descendants,
then schedules ready work through the same scheduler path as normal retries.
Before replaying, Fala checks the run provenance contract drift report. If the
stored pipeline contracts differ from the current registry, replay is rejected
unless the API body sets `allow_contract_drift: true` or the CLI uses
`--allow-contract-drift`.

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  dead-letter \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --operation-type ingest \
  --capability ingest_document

uv run fala \
  --pipeline-dir examples/pipelines \
  replay-dead-letter \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --document-id doc_1 \
  --process-id ingest \
  --reason "fixed source"
```

`stuck-work` lists non-terminal SLA breaches before they become terminal DLQ
items. It reports queued/waiting/running process instances that exceed thresholds,
retry backoff that is already due, and running claims whose lease expired:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  stuck-work \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --status running \
  --operation-type extract \
  --running-after-seconds 1800
```

When a step declares `sla`, those thresholds override the `stuck-work` query
defaults for queued, waiting, and running status checks.

Capability-level demand groups queue-backed worker needs across process ids:

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

The web panel exposes project readiness at `/project`, operator bootstrap
checks, DB schema status, and ready-to-run commands at `/project/bootstrap`,
one bootstrap runbook/spec at `/project/spec`, project-level health, backlog,
worker demand, and supervision summaries at `/project/operations`, project
alert policy at `/project/alerts`, project-level run-retention and artifact-GC
planning at `/project/lifecycle`, dead-letter, stuck-work, and stream-lag
triage at `/project/supervision`, and the same batch launcher at `/runs/new`.
It accepts uploaded files, document ids, URIs, and local server paths. Uploaded
files are stored in the content-addressed artifact
store and registered as
`fala-artifact://sha256/...` source refs. The panel
creates a run, registers all documents, initializes scheduler state, then
redirects to the run detail page. That page loads operator partials for live
runtime state, manual queue, dead-letter replay, stuck-work triage, run
provenance, run-level results, output documents, declared reductions, document
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
```

`manual-complete` accepts form fields `values` as a JSON object and optional
`metadata` as `key=value` lines, then uses the same process completion path as
workers.

Append more documents to an existing run with the same `RuntimeRunInput`
document contract:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  append-documents \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --pipeline document_processing \
  --existing-document reuse \
  --document attachment_2=s3://bucket/attachment_2.pdf
```

```text
POST /api/runs/{run_id}/process-runtime/documents/batch
```

Completed or failed runs can accept appended documents and recompute lifecycle
from the new runtime state. Cancelled runs reject appended documents. Paused
runs accept documents but still block new claims until resumed.
`--existing-document reuse` keeps existing document metadata and input intact,
then schedules any still-ready work.

## Event Streaming

Runtime events can be consumed as Server-Sent Events:

```text
GET /api/runs/{run_id}/process-runtime/events/stream
GET /api/runs/{run_id}/process-runtime/{document_id}/events/stream
```

Useful query parameters:

- `after_event_id`: resume after a known event id
- `process_id`: filter to one process
- `operation_type`: filter to one generic class of work
- `batch_limit`: max events read per store poll
- `max_events`: optional finite stream for tests and one-shot tools

Each process event is emitted as `event: process` with the serialized
`ProcessEvent` as JSON data. Event JSON includes `operation_type` when known.
Heartbeats are SSE comments.

## Chunked Streams

Processes can emit ordered stream chunks before final output or include
`stream_chunks` in their final `ProcessOutput`. This covers incremental document
work: OCR pages, email MIME parts, spreadsheet rows, image tiles, video frames,
generated storyboard scenes, or any other sequence. A chunk has `stream_id`,
`sequence`, optional `kind`, `values`, `artifacts`, and `metadata`. If the
process capability declares `emits_streams`, Fala validates the stream id,
optional chunk kind, `values`, `metadata`, and stream artifact kinds before
storing the chunk. Stream contracts can also declare `consumers`, which names
the expected checkpoint writers for operator lag views. Capabilities without
`emits_streams` keep the legacy open stream behavior.

Typed streams can also set `max_buffered_chunks`. Fala compares new chunks
against the slowest stored checkpoint for that stream. If appending a chunk would
push buffered chunks over the limit, the append fails with a backpressure error.
`package-doctor` warns when a backpressure-limited stream has no declared
consumers. This gives OCR/page extraction, email parsing, media rendering, and
LLM token streams a generic flow-control knob without binding Fala to one queue
engine.

Append and read chunks:

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

uv run fala \
  --pipeline-dir examples/pipelines \
  stream-list \
  --db /tmp/fala-example.db \
  --run-id run_batch_1 \
  --document-id contract.pdf \
  --process-id extract \
  --stream-id pages \
  --after-sequence 10
```

Track consumer progress:

```bash
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

Every appended chunk emits a `process.stream.chunk` event. Every checkpoint
update emits `process.stream.checkpoint`. This keeps the event log useful for
operators while stream payloads stay in their own cursorable store. Runtime
state includes per-step stream summaries with chunk counts, artifact counts,
latest sequence, value keys, kind counts, declared consumers, checkpoint
consumers, checkpoint lag, and step `operation_type`; the web panel shows those
summaries without loading every chunk payload.
The stream-lag read model lists lag by consumer, stream, document, process, and
operation type. It supports `operation_type` beside process, capability,
adapter, stream, and consumer filters. It also surfaces declared consumers that
have not checkpointed yet, missing checkpoints, and `max_buffered_chunks`
breaches for operators watching stream processing health.

External workers can use `ProcessRuntimeClient.read_stream_batch` and
`commit_stream_batch` for cursor-safe consumption:

```python
batch = await client.read_stream_batch(
    run_id=context["run_id"],
    document_id=context["document_id"],
    process_id="extract",
    stream_id="pages",
    consumer_id="chunk",
    limit=100,
)
for chunk in batch.chunks:
    await process_page(chunk.values)
await client.commit_stream_batch(batch)
```

`read_stream_batch` starts after the stored checkpoint for that consumer unless
`after_sequence` is passed explicitly. `commit_stream_batch` advances the
checkpoint to the last chunk in the batch and leaves empty batches unchanged.

## Operator Control

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

Body:

```json
{"action": "pause", "reason": "operator", "allow_contract_drift": false}
```

Paused runs keep current process state and reject new claims by returning an
empty claim response. Existing running claims are not cleared; workers may still
renew and finish their current process. `resume` recomputes run status from the
runtime state and allows claims again. Resume checks contract drift first and
requires `allow_contract_drift: true` / `--allow-contract-drift` when the stored
run contracts no longer match the current registry. `cancel` marks all unfinished
process instances as `cancelled`, clears active claims, blocks new claims, and
rejects late process output or stream writes. Completed and skipped process
instances are preserved.

Processes can be controlled through the API or web panel:

```text
POST /api/runs/{run_id}/process-runtime/{document_id}/processes/{process_id}/actions
```

Supported actions are `retry`, `skip`, `fail`, and `cancel`. Retry clears the
target process and descendants, then schedules ready work again. Automatic
retries from worker failure or claim expiry follow the step retry policy and
honor `delay_seconds`. Manual retry also checks contract drift and requires
`allow_contract_drift: true` when contracts changed. Skip writes a synthetic
skipped output. Fail and cancel move the process into terminal status. The web
panel exposes the same controls on each process card and refreshes the runtime
view after the action.

## Operator Audit

Fala records control-plane mutations in a separate operator audit log. This is
not the process event stream. It tracks who changed runtime state and where the
change came from: run creation/control, document scheduling, process control,
manual completion, output/status writes, stream writes, artifact GC, and run
retention requests.

API callers can set:

```text
X-Fala-Actor: operator@example.com
X-Fala-Source: backoffice
```

If headers are missing, API actions default to `api`; web panel actions default
to `web-panel`/`web`; worker writes use `worker:{worker_id}` when a worker id is
supplied.

When built-in auth is enabled, actor comes from the API-key principal instead of
caller-supplied actor headers. `X-Fala-Source` can still name the integration
source.

Read the log:

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

## Worker Supervisor

`supervise-workers` starts `process-runtime-worker` processes declared in
workflow package manifests and restarts them according to a restart policy.

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  supervise-workers \
  --base-url http://localhost:8000 \
  --run-id run_1 \
  --package-id basic_examples
```

Useful options:

- `--worker-id`: limit to one declared worker, repeatable
- `--restart-policy`: `never`, `on-failure`, or `always`
- `--max-restarts`: cap restart loops
- `--dry-run`: print supervised worker specs without starting processes

Render a full control-plane deployment from the same package worker declarations:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  deployment \
  --format docker-compose \
  --run-id run_1 \
  --package-id basic_examples \
  --image registry.example.com/fala:latest \
  --worker-image registry.example.com/fala-worker:latest \
  --with-postgres \
  --env FALA_API_KEYS=operator-secret:operator,worker-secret:worker \
  --worker-env FALA_API_KEY=worker-secret \
  | jq -r .manifest
```

The generated manifest includes the bundled API/web panel, shared artifact
volume, package workers pointed at the internal control-plane URL, and optional
Postgres. Docker Compose also gets a read-only pipeline bind mount when
`--container-pipeline-dir` is used. Use `--no-mount-pipeline-dir` when the image
already contains the package tree. `--format kubernetes` emits Deployment,
Service, and PVC YAML for the control plane and generated workers.

If the control plane already exists, render only worker manifests:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  worker-deployment \
  --base-url http://runtime.default.svc.cluster.local \
  --run-id run_1 \
  --package-id basic_examples \
  --format kubernetes \
  --image registry.example.com/fala-worker:latest \
  --replicas 2 \
  --namespace fala \
  --container-pipeline-dir /app/pipelines \
  --env FALA_DATABASE_URL=postgresql://postgres/fala \
  | jq -r .manifest
```

Use `--format docker-compose` for local or single-host deployment. Kubernetes
manifests translate worker `resources` into container requests where possible
and keep full Fala worker metadata in annotations.
Use `--container-pipeline-dir` when the package tree is mounted at a different
path inside the worker image. Fala rewrites the generated `--pipeline-dir`
argument and maps worker `cwd` values under the host pipeline directory into
that container path. Docker Compose manifests also get a read-only bind mount
from the host pipeline directory to `--container-pipeline-dir`; use
`--no-mount-pipeline-dir` when the image already contains the package tree. Use
`--container-workdir` to override the generated working directory explicitly.

Fala does not store secret values. Package `secrets` define ids and environment
names. Workers reference those ids. Docker Compose manifests emit host-env
placeholders such as `${OPENAI_API_KEY:?Fala secret openai_api_key is required}`;
Kubernetes manifests emit `env.valueFrom.secretKeyRef`. Worker `sandbox` renders
to Kubernetes container `securityContext` so generated deployments default
toward non-root, read-only root filesystem, no privilege escalation, dropped
capabilities, and `RuntimeDefault` seccomp.

Expose Prometheus text metrics for queue and autoscaling demand:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  metrics-prometheus \
  --db runtime.db \
  --run-id run_1 \
  | jq -r .metrics
```

```text
GET /api/runs/{run_id}/process-runtime/metrics/prometheus
```

Generate KEDA `ScaledObject` manifests that target
`fala_runtime_worker_target_workers` scraped by Prometheus:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  worker-autoscaling \
  --base-url http://runtime.default.svc.cluster.local \
  --run-id run_1 \
  --package-id basic_examples \
  --prometheus-server http://prometheus.monitoring.svc.cluster.local:9090 \
  --max-replicas 10 \
  --namespace fala \
  | jq -r .manifest
```

## Queue Bridge

`adapter.kind: queue` can be used in three modes:

- direct API workers with `process-runtime-worker`
- file bridge with `QueueWorkEnvelope` and `QueueResultEnvelope` JSONL
- durable local broker bridge backed by SQLite

The bridge keeps Fala broker-agnostic. Fala still owns claims, leases, retry
policy, process events, outputs, lineage, artifacts, and run state. The broker
only transports JSON envelopes.
When a `QueueResultEnvelope` has `status: failed`, `queue-apply-results` writes
it through the normal process status API. Fala then applies the step retry
policy: retryable error kinds reschedule work, terminal error kinds fail the
process, and the apply response includes the resulting process action.
Applying the same result envelope again is idempotent for already-applied
completed results and failed results whose `work_id` is already present in the
process event log. Fala returns `duplicate: true` instead of raising a conflict,
and it does not append progress events a second time.

Export claim envelopes:

```bash
uv run fala queue-export-claims \
  --base-url http://localhost:8000 \
  --run-id run_1 \
  --pipeline basic_enrichment \
  --worker-id bridge-publisher \
  --capability enrich_document \
  --max-claims 10 \
  --work-file work.jsonl
```

Run work from envelopes with a local command:

```bash
uv run fala queue-run-work \
  --work-file work.jsonl \
  --result-file results.jsonl \
  --command python steps/enrich.py
```

Apply results:

```bash
uv run fala queue-apply-results \
  --base-url http://localhost:8000 \
  --result-file results.jsonl
```

Use `--queue-broker` for a broker target. Plain paths and `sqlite://...` use
the local durable SQLite reference broker. `redis://...` and `rediss://...`
use Redis as a shared broker when `fala[redis]` is installed. Add
`?prefix=name` to isolate multiple Fala deployments in one Redis database.
`--queue-db` remains a compatibility alias for plain SQLite paths. Export writes
ready work rows, workers claim
leased rows, workers publish result rows, and apply marks result rows as
applied:

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

SQLite is the reference broker transport for local and small deployments.
Redis is the reference shared broker for multi-process and multi-worker
deployments. `memory://name` is the reference in-process broker for embedded
tests and web previews; it is not durable and does not coordinate separate
processes. SQS, Kafka, Pub/Sub, NATS, or a custom broker should implement
`QueueBrokerTransport` with the same envelope flow: publish work, lease/ack
work, publish results, apply results.
For long-running worker commands, pass `--renew-claim --base-url ...` to
`queue-run-work`. The broker worker then renews the original control-plane claim
while the command runs, so Fala does not expire and requeue the same process.
Use `queue-export-claims --unassigned-claim` when the exporter only publishes
work and another process executes it. In that mode the control-plane claim has
no worker owner until `queue-run-work --worker-id ... --renew-claim` assigns the
real broker worker. Without `--unassigned-claim`, the exported claim is owned by
the exporter worker id and result writes must use that same id.
Use `--max-deliveries` with broker transports to stop poison work from being
leased forever after repeated worker crashes. Once the delivery count is
exhausted, Fala moves that queue row to `dead_letter`; `queue-run-work` stats
include dead-letter counts, and `queue-list-work` can inspect row state plus
optional payloads:

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
`--keep-delivery-count` only when prior delivery attempts should still count
against `--max-deliveries`.
The web panel exposes the same operator flow on `/queue` when the app is created
with `queue_db=".fala/queue.sqlite"`, `FALA_QUEUE_BROKER=.fala/queue.sqlite`,
or `FALA_QUEUE_DB=.fala/queue.sqlite`. `fala serve --queue-broker ...` and
`fala deployment --queue-broker ...` wire the same target into local and
generated control-plane deployments.

Programmatic API:

```python
from fala import (
    JsonlQueueTransport,
    ProcessRuntimeClient,
    SQLiteQueueTransport,
    apply_queue_results,
    export_claims_to_queue,
    read_result_jsonl,
)

transport = JsonlQueueTransport(
    work_file="work.jsonl",
    result_file="results.jsonl",
)

async with ProcessRuntimeClient("http://localhost:8000") as client:
    await export_claims_to_queue(
        client,
        transport,
        run_id="run_1",
        pipeline_id="basic_enrichment",
        worker_id="bridge-publisher",
        capabilities=["enrich_document"],
        max_claims=10,
    )
    await apply_queue_results(client, read_result_jsonl("results.jsonl"))
```

Broker API:

```python
from fala import (
    AdapterRegistry,
	    ExternalCommandAdapter,
	    MemoryQueueTransport,
	    RedisQueueTransport,
	    SQLiteQueueTransport,
	    assign_queue_work_worker,
    create_queue_broker_transport,
    run_queue_work,
)

transport = SQLiteQueueTransport(".fala/queue.sqlite")
work = await transport.claim_work(queue="documents.enrich", worker_id="worker-1")
if work is not None:
    work = assign_queue_work_worker(work, "worker-1")
    adapters = AdapterRegistry.default()
    adapters.register(
        "queue",
        ExternalCommandAdapter(command=["python", "steps/enrich.py"]),
    )
    result = await run_queue_work(work, adapters=adapters)
    await transport.publish_result(result)
    await transport.complete_work(work.id)

dead_rows = await transport.list_work_records(
    state="dead_letter",
    include_payload=True,
)
if dead_rows:
    await transport.requeue_work(dead_rows[0].id)

preview_transport = create_queue_broker_transport("memory://preview")
assert isinstance(preview_transport, MemoryQueueTransport)
shared_transport = create_queue_broker_transport("redis://localhost/0?prefix=fala")
assert isinstance(shared_transport, RedisQueueTransport)
```

Use `fala schema queue-work-envelope`,
`fala schema queue-result-envelope`, and
`fala schema sqlite-queue-work-record` to generate contracts for broker
consumers, producers, and operator views.

## Artifact Store

Workers can still return local `file://` artifact refs. When output is written
through the runtime API, Fala materializes files under allowed artifact roots
into a content-addressed file store and persists refs like:

```text
fala-artifact://sha256/{digest}
```

Metadata includes `sha256`, `size_bytes`, `filename`, and storage details.
Process output lineage keeps input and dependency artifact summaries in
`metadata.process_runtime.lineage`, so operators can trace which document/source
artifacts contributed to an output even after file refs are materialized into the
content-addressed store.
Set `FALA_ARTIFACT_STORE` or `FALA_ARTIFACT_STORE_ROOT`, or pass
`artifact_store`/`artifact_store_root` to the runtime app factory/service to
choose the store. The default is `.flow-runs/artifact-store`. SDK helpers
resolve `fala-artifact://` refs from the same `FALA_ARTIFACT_STORE` target; for
remote stores they materialize blobs into `FALA_ARTIFACT_CACHE_ROOT` before
returning a local path.

`FALA_ARTIFACT_STORE` selects a backend target and takes precedence over
`FALA_ARTIFACT_STORE_ROOT`. Supported targets:

- plain path or `file:/path`: local content-addressed file store
- `memory://name`: in-memory content-addressed store for tests, previews, and
  embedded bootstrap flows
- `s3://bucket/prefix`: S3-compatible content-addressed object store. Install
  `fala[s3]` and configure boto3/AWS credentials, region, and optional endpoint
  through the standard environment.

Generated deployments accept `--artifact-store s3://bucket/prefix` and pass the
same target to the control plane and workers. Workers also receive
`FALA_ARTIFACT_CACHE_ROOT` for downloaded blob cache.

Hosts can inject any `ArtifactStore` implementation into `RuntimeService`. The
contract is content-addressed: `put_file`, `put_fileobj`, `open`, `resolve`,
`list_blobs`, and `delete_blobs`. This keeps S3/GCS/Azure/minio-style stores as
backend adapters, not scheduler or API special cases.

Artifact GC is explicit and dry-run by default:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  artifact-gc \
  --db /tmp/fala-example.db
```

The GC plan scans the content-addressed blob store and all runtime references in
the state store. Referenced blobs include registered document `source_uri`
values, document input artifacts, process output artifacts, and stream chunk
artifacts. `--delete` removes only orphaned blobs from the artifact store.

API:

```text
GET  /api/process-runtime/artifacts/gc
POST /api/process-runtime/artifacts/gc
```

Runtime state retention is separate from artifact GC. It is explicit and dry-run
by default:

```bash
uv run fala \
  --pipeline-dir examples/pipelines \
  run-retention \
  --db /tmp/fala-example.db \
  --older-than-days 30
```

Default selected statuses are terminal runs only: `completed`, `failed`, and
`cancelled`. Pass `--status` repeatedly to choose another set. `--delete`
removes selected run state from the runtime DB tables. It does not delete
artifact blobs; run `artifact-gc --delete` after state retention to reclaim blobs
that became orphaned.

API:

```text
GET  /api/process-runtime/runs/retention
POST /api/process-runtime/runs/retention
```

## FastAPI Integration

SQLite is enough for local development or a single-host tool:

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
`create_runtime_router` when run-level access checks are needed. Fala also ships
a small API-key policy for control-plane deployments:

```python
from fastapi import FastAPI
from fala import (
    PipelineRegistry,
    RuntimeAccessPolicy,
    RuntimeService,
    SQLiteStateStore,
    create_runtime_router,
)

registry = PipelineRegistry.from_directory("examples/pipelines")
store = SQLiteStateStore("runtime.db")
service = RuntimeService(registry=registry, store=store)
policy = RuntimeAccessPolicy.from_env()

app = FastAPI()
app.include_router(
    create_runtime_router(service, access_policy=policy),
    prefix="/api",
)
```

No `FALA_API_KEYS` means dev-open mode. Setting `FALA_API_KEYS` enables auth:

```bash
export FALA_API_KEYS='viewer-secret:viewer,worker-secret:worker,operator-secret:operator,admin-secret:admin'
```

Roles:

- `viewer`: read API and web panel
- `worker`: read plus claim/heartbeat/status/output/events/stream writes
- `operator`: worker permissions plus run creation, append, manual completion,
  pause/resume/cancel, process retry/skip/fail/cancel
- `admin`: all permissions, including destructive artifact GC and retention
  deletes

`FALA_API_KEYS` also accepts JSON for stable actors, sources, and tenant ids.
A key with `tenant_id` stamps new runs and cannot access runs stamped with
another tenant:

```bash
export FALA_API_KEYS='{
  "operator-secret": {"role": "operator", "actor": "ops@example.com", "tenant_id": "acme"},
  "worker-secret": {"role": "worker", "actor": "ocr-worker", "source": "keda"}
}'
```

Workers can pass `--api-key` or `FALA_API_KEY`; `ProcessRuntimeClient` accepts
`api_key=...`.

For a shared control plane with multiple workers, use PostgreSQL instead:

```python
import os

from fastapi import FastAPI
from fala import PipelineRegistry, PostgresStateStore, RuntimeService, create_runtime_router

registry = PipelineRegistry.from_directory("examples/pipelines")
store = PostgresStateStore(os.environ["FALA_DATABASE_URL"])
service = RuntimeService(registry=registry, store=store)

app = FastAPI()
app.include_router(create_runtime_router(service), prefix="/api")
```

Install the optional database client with `fala[postgres]`. The Postgres store
uses the same `StateStore` contract as SQLite, including run history, worker
heartbeats, queue claims, process outputs, stream chunks, checkpoints, lineage
inputs, and operator audit.

CLI commands that accept `--db` also accept a Postgres DSN:

```bash
uv run fala --pipeline-dir examples/pipelines \
  create-run \
  --db "$FALA_DATABASE_URL" \
  --run-input run-input.yaml

uv run fala db-doctor --db "$FALA_DATABASE_URL" --ensure-schema
```

`db-doctor` reports store kind, schema table coverage, current/latest schema
version, applied migrations, missing migrations, and runtime row counts. Without
`--ensure-schema` it checks the target as-is; with `--ensure-schema` it creates
or repairs the runtime schema before reporting.

The bundled web app uses `FALA_DATABASE_URL`, then `FALA_DB`, then `fala.db` when
no explicit store is supplied.

Run the bundled API and web panel directly:

```bash
uv run fala --pipeline-dir examples/pipelines \
  serve \
  --db fala.db \
  --host 127.0.0.1 \
  --port 8000
```

Generate a runnable stack with the web/API control plane, shared volumes,
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
artifact roots through the environment.

Postgres integration tests are opt-in so the default suite stays local-only:

```bash
FALA_POSTGRES_TEST_DSN="postgresql://fala:secret@localhost/fala_test" \
  uv run --extra postgres python -m unittest \
  tests.test_fala_runtime.ProcessRuntimeTests.test_postgres_state_store_live_runtime_contract_when_configured
```
