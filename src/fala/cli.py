from __future__ import annotations

import argparse
import asyncio
import csv
import fnmatch
import hashlib
import io
import json
import mimetypes
import os
import pprint
import shlex
import shutil
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from ipaddress import ip_address
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import unquote, urlparse

import yaml
from pydantic import BaseModel

from fala.auth import RuntimeAccessPolicy
from fala.adapters import AdapterRegistry, ExternalCommandAdapter, ProcessAdapterError
from fala.blueprints import (
    SCAFFOLD_BLUEPRINTS,
    ScaffoldBlueprint,
    document_source_value_schema,
    get_scaffold_blueprint,
    list_scaffold_blueprints,
    scaffold_blueprint_from_mapping,
    scaffold_blueprint_summary,
)
from fala.contract_lint import (
    discover_step_contracts,
    lint_step_contracts,
    load_step_contract_refs,
)
from fala.deployment import render_control_plane_deployment_manifest
from fala.deployment import render_worker_deployment_manifest
from fala.deployment import render_worker_autoscaling_manifest
from fala.gates import run_gate_suite_from_file
from fala.intake import (
    auto_document_routes_from_registry,
    coerce_document_routes,
    route_runtime_documents_with_report,
)
from fala.metrics import render_prometheus_metrics
from fala.operations import (
    operation_type_for_step,
    operation_type_spec,
)
from fala.models import (
    ArtifactKindSpec,
    ArtifactRef,
    CapabilitySpec,
    CombinedProjection,
    CombineSpec,
    DocumentTypeSpec,
    DocumentRelationSpec,
    OperatorAuditEvent,
    OperatorAuditEventPage,
    OperationTypeSpec,
    PipelineSpec,
    ProcessAction,
    ProcessActionInput,
    ProcessConditionSpec,
    ProcessEvent,
    ProcessEventPage,
    ProcessExecutionContext,
    ProcessOutput,
    ProcessOutputStreamChunk,
    ProcessSlaSpec,
    ProcessSpec,
    ProcessStatus,
    ResourcePoolSpec,
    ResourceQuantity,
    ResourceSpec,
    RunReduceSpec,
    RuntimeArtifactGcPlan,
    RuntimeCapabilityDemand,
    RuntimeCapabilityDemandSummary,
    RuntimeDeadLetterItem,
    RuntimeDeadLetterPage,
    RuntimeDocumentInput,
    RuntimeDocumentPage,
    RuntimeDocumentStatus,
    RuntimeDocumentLineage,
    RuntimeDocumentStepReport,
    RuntimeOutputDocumentPage,
    RuntimeProcessPage,
    RuntimeProcessRecord,
    RuntimeQueueMetrics,
    RuntimeRunHealth,
    RuntimeRunInput,
    RuntimeRunReductions,
    RuntimeRunRetentionPlan,
    RuntimeRunResults,
    RunStatus,
    RuntimeStuckWorkItem,
    RuntimeStuckWorkPage,
    RuntimeState,
    RuntimeStepReport,
    RuntimeStepReportItem,
    RuntimeStepReportSummary,
    RuntimeStreamBatch,
    RuntimeStreamCheckpoint,
    RuntimeStreamChunk,
    RuntimeStreamLagItem,
    RuntimeStreamLagPage,
    RuntimeStreamSnapshot,
    RuntimeTrace,
    RuntimeWorkerHeartbeat,
    RuntimeWorkerDemand,
    RuntimeWorkerState,
    SpawnDocumentInput,
    StreamSpec,
    WorkflowPackageSpec,
    WorkflowWorkerSpec,
    WorkflowSecretSpec,
    WorkerSandboxSpec,
)
from fala.package_registry import (
    PackageFileDigest,
    PipelineRelease,
    WorkflowPackageReadiness,
    WorkflowPackageReadinessIssue,
    WorkflowPackageRelease,
    WorkflowReadinessReport,
    WorkflowRegistryIndex,
    build_workflow_readiness_report,
    build_workflow_registry_index,
)
from fala.project import (
    build_project_alert_report,
    build_project_bootstrap_check,
    build_project_lifecycle_report,
    build_project_operations_report,
    build_project_readiness_report,
    build_project_run_history,
    build_project_runtime_run_input,
    build_project_secret_inventory,
    build_project_spec_report,
    build_project_supervision_report,
    project_pipeline_dir,
    render_project_env_template,
    verify_project_bundle,
    write_project_bundle,
)
from fala.queue_bridge import (
    QueueResultEnvelope,
    QueueWorkEnvelope,
    SQLiteQueueWorkRecord,
    apply_queue_results,
    assign_queue_work_worker,
    create_queue_broker_transport,
    export_claims_to_queue,
    read_result_jsonl,
    read_work_jsonl,
    run_queue_work,
    write_jsonl,
)
from fala.registry import PipelineRegistry
from fala.runtime_backend import GateStatus as CarrierGateStatus
from fala.runtime_backend import SQLiteRuntimeBackend
from fala.scheduler import PipelineScheduler, ScheduleResult
from fala.sdk import replay_step_manifest
from fala.service import RuntimeService
from fala.state import build_runtime_document_state, build_runtime_state
from fala.step_bundle import verify_step_replay_bundle, write_step_replay_bundle
from fala.store import InMemoryStateStore, StateStore
from fala.store_factory import create_state_store, runtime_db_diagnostics
from fala.supervisor import ProcessSupervisor, build_package_worker_specs
from fala.yaml_loader import pipeline_from_mapping, workflow_package_from_mapping

ADAPTER_KIND_CHOICES = ("subprocess", "http", "queue", "manual")
CONTRACT_MODELS: dict[str, type[BaseModel]] = {
    "package-file-digest": PackageFileDigest,
    "pipeline": PipelineSpec,
    "pipeline-release": PipelineRelease,
    "process-context": ProcessExecutionContext,
    "process-output": ProcessOutput,
    "process-output-stream-chunk": ProcessOutputStreamChunk,
    "process-sla": ProcessSlaSpec,
    "queue-result-envelope": QueueResultEnvelope,
    "sqlite-queue-work-record": SQLiteQueueWorkRecord,
    "queue-work-envelope": QueueWorkEnvelope,
    "resource": ResourceSpec,
    "resource-pool": ResourcePoolSpec,
    "resource-quantity": ResourceQuantity,
    "process-action": ProcessActionInput,
    "process-condition": ProcessConditionSpec,
    "operator-audit-event": OperatorAuditEvent,
    "operator-audit-event-page": OperatorAuditEventPage,
    "run-reduce": RunReduceSpec,
    "artifact": ArtifactRef,
    "artifact-kind": ArtifactKindSpec,
    "capability": CapabilitySpec,
    "document-type": DocumentTypeSpec,
    "document-relation": DocumentRelationSpec,
    "operation-type": OperationTypeSpec,
    "event": ProcessEvent,
    "event-page": ProcessEventPage,
    "runtime-document-input": RuntimeDocumentInput,
    "runtime-document-page": RuntimeDocumentPage,
    "runtime-output-document-page": RuntimeOutputDocumentPage,
    "runtime-process-page": RuntimeProcessPage,
    "runtime-process-record": RuntimeProcessRecord,
    "runtime-artifact-gc-plan": RuntimeArtifactGcPlan,
    "runtime-capability-demand": RuntimeCapabilityDemand,
    "runtime-capability-demand-summary": RuntimeCapabilityDemandSummary,
    "runtime-dead-letter-item": RuntimeDeadLetterItem,
    "runtime-dead-letter-page": RuntimeDeadLetterPage,
    "runtime-document-lineage": RuntimeDocumentLineage,
    "runtime-document-step-report": RuntimeDocumentStepReport,
    "spawn-document-input": SpawnDocumentInput,
    "stream": StreamSpec,
    "runtime-queue-metrics": RuntimeQueueMetrics,
    "runtime-run-health": RuntimeRunHealth,
    "runtime-run-input": RuntimeRunInput,
    "runtime-run-reductions": RuntimeRunReductions,
    "runtime-run-retention-plan": RuntimeRunRetentionPlan,
    "runtime-run-results": RuntimeRunResults,
    "runtime-stuck-work-item": RuntimeStuckWorkItem,
    "runtime-stuck-work-page": RuntimeStuckWorkPage,
    "runtime-state": RuntimeState,
    "runtime-step-report": RuntimeStepReport,
    "runtime-step-report-item": RuntimeStepReportItem,
    "runtime-step-report-summary": RuntimeStepReportSummary,
    "runtime-stream-batch": RuntimeStreamBatch,
    "runtime-stream-checkpoint": RuntimeStreamCheckpoint,
    "runtime-stream-chunk": RuntimeStreamChunk,
    "runtime-stream-lag-item": RuntimeStreamLagItem,
    "runtime-stream-lag-page": RuntimeStreamLagPage,
    "runtime-stream-snapshot": RuntimeStreamSnapshot,
    "runtime-trace": RuntimeTrace,
    "runtime-worker-heartbeat": RuntimeWorkerHeartbeat,
    "runtime-worker-demand": RuntimeWorkerDemand,
    "runtime-worker-state": RuntimeWorkerState,
    "workflow-package": WorkflowPackageSpec,
    "workflow-package-readiness": WorkflowPackageReadiness,
    "workflow-package-readiness-issue": WorkflowPackageReadinessIssue,
    "workflow-package-release": WorkflowPackageRelease,
    "workflow-readiness-report": WorkflowReadinessReport,
    "workflow-secret": WorkflowSecretSpec,
    "workflow-registry-index": WorkflowRegistryIndex,
    "worker-sandbox": WorkerSandboxSpec,
}


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command == "serve":
        try:
            return _serve_runtime_web(args)
        except Exception as exc:
            print(f"error: {exc}")
            return 1

    try:
        payload = asyncio.run(_run(args))
    except Exception as exc:
        if _should_emit_json_error(args):
            print(json.dumps({"ok": False, "error": str(exc)}, indent=2, sort_keys=True))
        else:
            print(f"error: {exc}")
        return 1

    if payload is not None:
        print(json.dumps(payload, indent=2, sort_keys=True))
        if isinstance(payload, dict) and payload.get("ok") is False:
            return 1
    return 0


def _should_emit_json_error(args: argparse.Namespace) -> bool:
    return bool(
        getattr(args, "json", False)
        or getattr(args, "command", None)
        in {
            "schema",
            "validate-output",
            "validate-context",
            "discover-documents",
            "db-doctor",
            "inspect-run-input",
            "plan-run",
            "validate-run",
            "health",
            "audit-log",
            "append-documents",
            "carriers",
            "list-documents",
            "list-processes",
            "dead-letter",
            "events",
            "gates",
            "observations",
            "package-doctor",
            "projections",
            "project-alerts",
            "project-bundle",
            "project-bundle-verify",
            "project-check",
            "project-lifecycle",
            "project-operations",
            "project-secrets",
            "project-smoke",
            "project-supervision",
            "replay-dead-letter",
            "run-gates",
            "stuck-work",
            "queue-metrics",
            "capability-demands",
            "contract-lint",
            "step-bundle",
            "step-bundle-verify",
            "stream-append",
            "stream-checkpoint",
            "stream-checkpoint-get",
            "stream-lag",
            "stream-list",
            "step-replay",
            "trace",
        }
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="process-runtime")
    parser.add_argument(
        "--pipeline-dir",
        default=None,
        help="Directory with *.yaml pipeline definitions.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_project_base_args(command: argparse.ArgumentParser) -> None:
        command.add_argument(
            "--project-dir",
            default=".",
            help="Project root. Defaults to current directory.",
        )
        command.add_argument(
            "--project-yaml",
            default=None,
            help="Project manifest path. Defaults to PROJECT_DIR/fala-project.yaml.",
        )

    def add_project_runtime_args(command: argparse.ArgumentParser) -> None:
        command.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
        add_project_base_args(command)
        command.add_argument("--package-id", default=None)
        command.add_argument("--pipeline-id", default=None)
        command.add_argument("--document-type", default=None)
        command.add_argument("--operation-type", default=None)
        command.add_argument(
            "--run-limit",
            type=int,
            default=500,
            help="Maximum project run summaries to inspect.",
        )
        command.add_argument(
            "--limit",
            type=int,
            default=50,
            help="Maximum detail rows to include.",
        )

    def add_project_supervision_args(command: argparse.ArgumentParser) -> None:
        add_project_runtime_args(command)
        command.add_argument(
            "--stuck-status",
            choices=[status.value for status in ProcessStatus],
            default=None,
        )
        command.add_argument("--waiting-after-seconds", type=float, default=3600.0)
        command.add_argument("--queued-after-seconds", type=float, default=600.0)
        command.add_argument("--running-after-seconds", type=float, default=1800.0)
        command.add_argument("--consumer-id", default=None)
        command.add_argument("--min-lag", type=int, default=1)
        command.add_argument(
            "--over-limit",
            action=argparse.BooleanOptionalAction,
            default=None,
            help="Only include streams over their configured lag limit.",
        )

    def add_project_output_arg(command: argparse.ArgumentParser, label: str) -> None:
        command.add_argument(
            "--output",
            default=None,
            help=f"Write JSON {label} to this path instead of stdout envelope.",
        )

    validate = subparsers.add_parser("validate", help="Validate pipeline YAML files.")
    validate.add_argument("--json", action="store_true", help="Emit JSON result.")
    validate.add_argument(
        "--check-commands",
        action="store_true",
        help="Also verify subprocess executables are available in this environment.",
    )

    schema = subparsers.add_parser("schema", help="Emit JSON Schema for a runtime contract model.")
    schema.add_argument("model", choices=sorted(CONTRACT_MODELS))

    validate_output = subparsers.add_parser(
        "validate-output",
        help="Validate ProcessOutput JSON from a file or stdin.",
    )
    validate_output.add_argument("--file", default="-", help="JSON file path, or '-' for stdin.")

    validate_context = subparsers.add_parser(
        "validate-context",
        help="Validate ProcessExecutionContext JSON from a file or stdin.",
    )
    validate_context.add_argument("--file", default="-", help="JSON file path, or '-' for stdin.")

    run_gates = subparsers.add_parser(
        "run-gates",
        help="Run generic quality gates from a YAML config.",
    )
    run_gates.add_argument("--config", required=True, help="Gate suite YAML file.")
    run_gates.add_argument(
        "--base-dir",
        default=None,
        help="Base directory for relative artifact paths. Defaults to the config directory.",
    )
    run_gates.add_argument(
        "--evidence-output",
        default=None,
        help="Optional evidence-pack JSON path to write.",
    )
    run_gates.add_argument(
        "--output",
        default=None,
        help="Optional gate report JSON path to write.",
    )

    step_replay = subparsers.add_parser(
        "step-replay",
        help="Replay a local step command from a step_run_manifest.json context.",
    )
    step_replay.add_argument("--manifest", required=True)
    step_replay.add_argument("--cwd", default=None)
    step_replay.add_argument("--env", action="append", default=[])
    step_replay.add_argument("--timeout-seconds", type=float, default=None)
    step_replay.add_argument(
        "exec_command",
        nargs=argparse.REMAINDER,
        help="Step command. Prefix with -- when needed.",
    )

    step_bundle = subparsers.add_parser(
        "step-bundle",
        help="Write a portable replay bundle from a step_run_manifest.json context.",
    )
    step_bundle.add_argument("--manifest", required=True)
    step_bundle.add_argument(
        "--output",
        default="step-replay-bundle.tar.gz",
        help="Archive path to write.",
    )
    step_bundle.add_argument("--cwd", default=None)
    step_bundle.add_argument("--env", action="append", default=[])
    step_bundle.add_argument(
        "--bundle-name",
        default=None,
        help="Top-level directory name inside the archive.",
    )
    step_bundle.add_argument(
        "exec_command",
        nargs=argparse.REMAINDER,
        help="Step command. Prefix with -- when needed.",
    )

    step_bundle_verify = subparsers.add_parser(
        "step-bundle-verify",
        help="Verify a step replay bundle archive without extracting it.",
    )
    step_bundle_verify.add_argument("bundle", help="Bundle tar.gz path.")

    inspect_run_input = subparsers.add_parser(
        "inspect-run-input",
        help="Inspect a RuntimeRunInput JSON/YAML manifest without creating a run.",
    )
    inspect_run_input.add_argument(
        "--run-input",
        required=True,
        help="RuntimeRunInput JSON/YAML manifest path, or '-' for stdin.",
    )

    contract = subparsers.add_parser(
        "contract",
        help="Emit typed preflight contract for one pipeline.",
    )
    contract.add_argument("pipeline_id")

    contract_lint = subparsers.add_parser(
        "contract-lint",
        help="Compare Python StepContract objects with one pipeline contract.",
    )
    contract_lint.add_argument("--pipeline", required=True)
    contract_lint.add_argument(
        "--contract",
        action="append",
        default=[],
        help=(
            "Python contract reference, as module[:attribute]. Attribute defaults "
            "to CONTRACT and may be a StepContract or iterable of StepContract."
        ),
    )
    contract_lint.add_argument(
        "--python-path",
        action="append",
        default=[],
        help="Path to prepend to sys.path before importing --contract refs.",
    )
    contract_lint.add_argument(
        "--no-discover-contracts",
        action="store_true",
        help="Disable convention-based discovery of contracts.py and *_contracts.py.",
    )
    contract_lint.add_argument(
        "--allow-missing",
        action="store_true",
        help="Do not fail when pipeline steps have no supplied StepContract.",
    )

    package_index = subparsers.add_parser(
        "package-index",
        help="Emit versioned workflow package release index with digests.",
    )
    package_index.add_argument("--package-id", default=None)
    package_index.add_argument(
        "--output",
        default=None,
        help="Write JSON index to this path instead of stdout envelope.",
    )

    db_doctor = subparsers.add_parser(
        "db-doctor",
        help="Check runtime database connectivity and schema readiness.",
    )
    db_doctor.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    db_doctor.add_argument(
        "--ensure-schema",
        action="store_true",
        help="Create/repair runtime schema before checking.",
    )
    db_doctor.add_argument(
        "--output",
        default=None,
        help="Write JSON database report to this path instead of stdout envelope.",
    )

    package_doctor = subparsers.add_parser(
        "package-doctor",
        help="Report workflow package readiness for bootstrapping document projects.",
    )
    package_doctor.add_argument("--package-id", default=None)
    package_doctor.add_argument(
        "--output",
        default=None,
        help="Write JSON readiness report to this path instead of stdout envelope.",
    )
    package_doctor.add_argument(
        "--contract",
        action="append",
        default=[],
        help=(
            "Optional Python StepContract reference for package doctor, as "
            "module[:attribute]. Convention-based discovery runs when no "
            "--contract is supplied."
        ),
    )
    package_doctor.add_argument(
        "--python-path",
        action="append",
        default=[],
        help="Path to prepend to sys.path before importing or discovering contracts.",
    )
    package_doctor.add_argument(
        "--no-discover-contracts",
        action="store_true",
        help="Disable convention-based StepContract discovery in package doctor.",
    )

    scaffold_blueprints = subparsers.add_parser(
        "scaffold-blueprints",
        help="List built-in generic scaffold blueprints.",
    )
    scaffold_blueprints.add_argument(
        "--blueprint",
        choices=tuple(SCAFFOLD_BLUEPRINTS),
        default=None,
        help="Show one blueprint instead of the full catalog.",
    )
    scaffold_blueprints.add_argument(
        "--blueprint-file",
        default=None,
        help="Inspect a custom scaffold blueprint YAML file.",
    )
    scaffold_blueprints.add_argument(
        "--query",
        default=None,
        help="Filter the built-in blueprint catalog by text.",
    )

    init_project = subparsers.add_parser(
        "init-project",
        help="Create a multi-package document-work workspace from blueprints.",
    )
    init_project.add_argument("--output-dir", required=True, help="Project directory to create.")
    init_project.add_argument(
        "--project-id",
        required=True,
        help="Project id used in generated root README and Makefile.",
    )
    init_project.add_argument(
        "--blueprint",
        action="append",
        default=[],
        choices=tuple(SCAFFOLD_BLUEPRINTS),
        help=(
            "Named blueprint to include. Repeatable. Defaults to every built-in "
            "blueprint."
        ),
    )
    init_project.add_argument(
        "--blueprint-file",
        action="append",
        default=[],
        help="Custom scaffold blueprint YAML file to include. Repeatable.",
    )
    init_project.add_argument(
        "--adapter-kind",
        choices=("subprocess", "queue"),
        default="subprocess",
        help="Adapter kind to write in generated package pipeline YAML.",
    )

    project_doctor = subparsers.add_parser(
        "project-doctor",
        help="Report root Fala project readiness from fala-project.yaml.",
    )
    project_doctor.add_argument(
        "--project-dir",
        default=".",
        help="Project root. Defaults to current directory.",
    )
    project_doctor.add_argument(
        "--project-yaml",
        default=None,
        help="Project manifest path. Defaults to PROJECT_DIR/fala-project.yaml.",
    )
    project_doctor.add_argument(
        "--output",
        default=None,
        help="Write JSON readiness report to this path instead of stdout envelope.",
    )
    project_spec = subparsers.add_parser(
        "project-spec",
        help="Export project bootstrap runbook/spec from fala-project.yaml.",
    )
    project_spec.add_argument(
        "--project-dir",
        default=".",
        help="Project root. Defaults to current directory.",
    )
    project_spec.add_argument(
        "--project-yaml",
        default=None,
        help="Project manifest path. Defaults to PROJECT_DIR/fala-project.yaml.",
    )
    project_spec.add_argument(
        "--base-url",
        default="http://localhost:8000",
        help="Control plane base URL for generated worker commands.",
    )
    project_spec.add_argument(
        "--run-id",
        default=None,
        help="Run id for generated worker commands. Defaults to project run_id.",
    )
    project_spec.add_argument(
        "--output",
        default=None,
        help="Write JSON spec to this path instead of stdout envelope.",
    )

    project_check = subparsers.add_parser(
        "project-check",
        help="Run aggregate project bootstrap checks.",
    )
    add_project_base_args(project_check)
    project_check.add_argument("--db", default=None, help="Optional runtime DB path or DSN.")
    project_check.add_argument(
        "--ensure-schema",
        action="store_true",
        help="Create/repair runtime schema before DB check.",
    )
    project_check.add_argument(
        "--bundle",
        default=None,
        help="Optional project bundle tar.gz to verify as part of the report.",
    )
    project_check.add_argument(
        "--base-url",
        default="http://localhost:8000",
        help="Control plane base URL for generated worker commands.",
    )
    project_check.add_argument(
        "--run-id",
        default=None,
        help="Run id for generated worker commands. Defaults to project run_id.",
    )
    project_check.add_argument(
        "--output",
        default=None,
        help="Write JSON project check report to this path instead of stdout envelope.",
    )

    project_smoke = subparsers.add_parser(
        "project-smoke",
        help="Create a mixed project run and execute local/declared workers until idle.",
    )
    add_project_base_args(project_smoke)
    project_smoke.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    project_smoke.add_argument("--run-id", default=None)
    project_smoke.add_argument("--title", default=None)
    project_smoke.add_argument("--worker-id", default="local-smoke")
    project_smoke.add_argument("--max-steps", type=int, default=1000)
    project_smoke.add_argument(
        "--existing-run",
        choices=("error", "resume"),
        default="error",
        help="Policy when --run-id already exists.",
    )
    project_smoke.add_argument(
        "--existing-document",
        choices=("error", "reuse"),
        default="error",
        help="Policy when a document_id already exists in the run.",
    )
    project_smoke.add_argument(
        "--adapter-kind",
        action="append",
        choices=("subprocess", "http", "queue"),
        default=[],
        help="Executable adapter kind to include. Repeatable. Defaults to all local kinds.",
    )
    project_smoke.add_argument(
        "--metadata",
        action="append",
        default=[],
        help="Run metadata as key=value. Repeatable.",
    )
    _add_resource_args(project_smoke)
    project_smoke.add_argument(
        "--include-state",
        action="store_true",
        help="Include full runtime state in the smoke report.",
    )
    project_smoke.add_argument(
        "--output",
        default=None,
        help="Write JSON smoke report to this path instead of stdout envelope.",
    )

    project_secrets = subparsers.add_parser(
        "project-secrets",
        help="Export project worker secret inventory and optional .env template.",
    )
    add_project_base_args(project_secrets)
    project_secrets.add_argument(
        "--output",
        default=None,
        help="Write JSON secret inventory to this path instead of stdout envelope.",
    )
    project_secrets.add_argument(
        "--env-output",
        default=None,
        help="Write .env.example style template to this path.",
    )
    project_secrets.add_argument(
        "--no-auth-placeholders",
        action="store_true",
        help="Do not include optional FALA_API_KEYS/FALA_API_KEY comments.",
    )

    project_bundle = subparsers.add_parser(
        "project-bundle",
        help="Write a portable project archive without runtime DB or secret values.",
    )
    add_project_base_args(project_bundle)
    project_bundle.add_argument(
        "--output",
        default="fala-project-bundle.tar.gz",
        help="Archive path to write.",
    )
    project_bundle.add_argument(
        "--base-url",
        default="http://localhost:8000",
        help="Control plane base URL for bundled project-spec worker commands.",
    )
    project_bundle.add_argument(
        "--run-id",
        default=None,
        help="Run id for bundled project-spec worker commands.",
    )
    project_bundle.add_argument(
        "--bundle-name",
        default=None,
        help="Top-level directory name inside the archive.",
    )

    project_bundle_verify = subparsers.add_parser(
        "project-bundle-verify",
        help="Verify a project bundle archive without extracting it.",
    )
    project_bundle_verify.add_argument("bundle", help="Bundle tar.gz path.")

    project_supervision = subparsers.add_parser(
        "project-supervision",
        help="Report project dead-letter, stuck-work, and stream-lag across runs.",
    )
    add_project_supervision_args(project_supervision)
    add_project_output_arg(project_supervision, "project supervision report")

    project_operations = subparsers.add_parser(
        "project-operations",
        help="Report project health, backlog, workers, and supervision summary.",
    )
    add_project_supervision_args(project_operations)
    project_operations.add_argument("--stale-after-seconds", type=float, default=60.0)
    add_project_output_arg(project_operations, "project operations report")

    project_alerts = subparsers.add_parser(
        "project-alerts",
        help="Evaluate fala-project.yaml alert rules over project operations.",
    )
    add_project_supervision_args(project_alerts)
    project_alerts.add_argument("--stale-after-seconds", type=float, default=60.0)
    add_project_output_arg(project_alerts, "project alerts report")

    project_lifecycle = subparsers.add_parser(
        "project-lifecycle",
        help="Plan or delete project run-retention and artifact-GC candidates.",
    )
    add_project_runtime_args(project_lifecycle)
    project_lifecycle.add_argument("--before", default=None, help="ISO datetime cutoff.")
    project_lifecycle.add_argument(
        "--older-than-days",
        type=float,
        default=None,
        help="Override lifecycle.run_retention.older_than_days.",
    )
    project_lifecycle.add_argument(
        "--status",
        action="append",
        default=[],
        choices=[status.value for status in RunStatus],
        help="Run status to include. Repeatable. Defaults to project policy.",
    )
    project_lifecycle.add_argument(
        "--skip-artifact-gc",
        action="store_true",
        help="Skip artifact-GC planning.",
    )
    project_lifecycle.add_argument(
        "--artifact-store-root",
        default=None,
        help="Artifact store root for artifact-GC planning.",
    )
    project_lifecycle.add_argument(
        "--delete",
        action="store_true",
        help="Delete selected runs. Without this flag, only reports a dry-run plan.",
    )
    add_project_output_arg(project_lifecycle, "project lifecycle report")

    scaffold = subparsers.add_parser(
        "scaffold",
        help="Create a workflow package with one SDK-backed program per step.",
    )
    scaffold.add_argument("--output-dir", required=True, help="Directory to create.")
    scaffold.add_argument("--package-id", required=True, help="Workflow package id.")
    scaffold.add_argument("--pipeline-id", required=True, help="Pipeline id.")
    scaffold_mode = scaffold.add_mutually_exclusive_group(required=True)
    scaffold_mode.add_argument(
        "--steps",
        help="Comma-separated process ids. Generates one program per id.",
    )
    scaffold_mode.add_argument(
        "--blueprint",
        choices=tuple(SCAFFOLD_BLUEPRINTS),
        help="Named generic workflow blueprint to scaffold.",
    )
    scaffold_mode.add_argument(
        "--blueprint-file",
        help="Custom scaffold blueprint YAML file.",
    )
    scaffold.add_argument(
        "--adapter-kind",
        choices=("subprocess", "queue"),
        default="subprocess",
        help="Adapter kind to write in generated pipeline YAML.",
    )
    scaffold.add_argument("--title", default=None, help="Optional package and pipeline title.")
    scaffold.add_argument(
        "--document-type",
        default=None,
        help="Document type id declared by the generated package.",
    )
    scaffold.add_argument(
        "--document-media-type",
        action="append",
        default=[],
        help=(
            "Media type accepted by the generated document type. Repeatable. "
            "Defaults to application/octet-stream."
        ),
    )
    scaffold.add_argument(
        "--document-extension",
        action="append",
        default=[],
        help=(
            "File extension accepted by the generated document type. Repeatable. "
            "Include dot or omit it."
        ),
    )
    scaffold.add_argument(
        "--artifact-extension",
        action="append",
        default=[],
        help=(
            "File extension accepted by one generated artifact kind as STEP=EXT. "
            "Repeatable. Include dot or omit it."
        ),
    )
    scaffold.add_argument(
        "--artifact-value-schema",
        action="append",
        default=[],
        help="JSON/YAML artifact value schema as STEP=PATH. Repeatable.",
    )
    scaffold.add_argument(
        "--capability-output-schema",
        action="append",
        default=[],
        help="JSON/YAML capability output schema as STEP=PATH. Repeatable.",
    )
    scaffold.add_argument(
        "--stream-contract",
        action="append",
        default=[],
        help=(
            "JSON/YAML stream contract file as STEP=PATH. "
            "File may contain one stream, a list, or {streams: [...]}."
        ),
    )
    scaffold.add_argument(
        "--step-policy",
        action="append",
        default=[],
        help=(
            "JSON/YAML process policy as STEP=PATH. Supports title, adapter, retry, "
            "resources, sla, when, priority, max_concurrency, resource_pool, config."
        ),
    )
    scaffold.add_argument(
        "--document-value-schema",
        default=None,
        help="JSON/YAML schema path for generated document input values.",
    )
    scaffold.add_argument(
        "--document-metadata-schema",
        default=None,
        help="JSON/YAML schema path for generated document metadata.",
    )

    sync_contracts = subparsers.add_parser(
        "sync-contracts",
        help="Apply editable contracts/ files back to package and pipeline YAML.",
    )
    sync_contracts.add_argument("--package-yaml", required=True)
    sync_contracts.add_argument("--pipeline-yaml", required=True)
    sync_contracts.add_argument("--contract-dir", required=True)

    discover = subparsers.add_parser(
        "discover-documents",
        help="Build a RuntimeRunInput manifest from local files.",
    )
    discover.add_argument(
        "--input-dir",
        action="append",
        default=[],
        help="Directory to scan. Repeatable.",
    )
    discover.add_argument(
        "--file",
        action="append",
        default=[],
        help="Specific local file to include. Repeatable.",
    )
    discover.add_argument(
        "--source-list",
        action="append",
        default=[],
        help="CSV/TSV source list with source_uri or path columns. Repeatable.",
    )
    discover.add_argument(
        "--include",
        action="append",
        default=[],
        help="fnmatch pattern for relative file paths. Repeatable. Defaults to '*'.",
    )
    discover.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="fnmatch pattern for relative file paths to skip. Repeatable.",
    )
    discover.add_argument(
        "--no-recursive",
        action="store_true",
        help="Only scan direct children of each --input-dir.",
    )
    discover.add_argument(
        "--content-hash",
        action="store_true",
        help="Compute SHA-256 for local files and store it as source_sha256 metadata.",
    )
    discover.add_argument(
        "--document-id-mode",
        choices=("path", "name", "sha256"),
        default="path",
        help="How to auto-generate document_id when one is not explicit.",
    )
    discover.add_argument(
        "--pipeline",
        default=None,
        help=(
            "Default pipeline id. Optional when --route or source-list rows set "
            "per-document pipeline_id."
        ),
    )
    discover.add_argument(
        "--route",
        action="append",
        default=[],
        help=(
            "YAML/JSON document routing rules. Repeatable. First matching rule "
            "fills missing pipeline_id/document_type/media_type/values/metadata."
        ),
    )
    discover.add_argument(
        "--auto-route",
        action="store_true",
        help=(
            "Infer missing per-document pipeline_id/document_type from workflow "
            "package document type contracts in --pipeline-dir."
        ),
    )
    discover.add_argument(
        "--route-report",
        default=None,
        help=(
            "Write route diagnostics JSON to this path while stdout remains the "
            "RuntimeRunInput manifest."
        ),
    )
    discover.add_argument("--run-id", default=None)
    discover.add_argument("--title", default=None)
    discover.add_argument("--document-type", default=None)
    discover.add_argument("--media-type", default=None)
    discover.add_argument(
        "--existing-run",
        choices=("error", "resume"),
        default="error",
        help="Policy when --run-id already exists.",
    )
    discover.add_argument(
        "--existing-document",
        choices=("error", "reuse"),
        default="error",
        help="Policy when a document_id already exists in the run.",
    )
    discover.add_argument(
        "--value",
        action="append",
        default=[],
        help="Initial value copied to each discovered document as key=value.",
    )
    discover.add_argument(
        "--metadata",
        action="append",
        default=[],
        help="Metadata copied to each discovered document as key=value.",
    )

    subparsers.add_parser("list", help="List pipeline ids.")

    serve = subparsers.add_parser(
        "serve",
        help="Run the bundled API and web control plane.",
    )
    serve.add_argument("--host", default="127.0.0.1", help="Bind host.")
    serve.add_argument("--port", type=int, default=8000, help="Bind port.")
    serve.add_argument(
        "--db",
        default=None,
        help=(
            "Runtime SQLite DB path or sqlite:// URL. Defaults to FALA_DATABASE_URL, "
            "FALA_DB, then fala.db."
        ),
    )
    serve.add_argument(
        "--queue-broker",
        default=None,
        help=(
            "Queue broker target. Supports memory://name, redis://..., "
            "sqlite://path, or plain SQLite paths. Defaults to FALA_QUEUE_BROKER."
        ),
    )
    serve.add_argument(
        "--queue-db",
        default=None,
        help="SQLite queue broker DB path. Compatibility alias for --queue-broker.",
    )
    serve.add_argument(
        "--artifact-root",
        action="append",
        default=[],
        help=(
            "Allowed source artifact root for local file uploads. Repeatable. "
            "FALA_ARTIFACT_ROOTS is also honored by the runtime service."
        ),
    )
    serve.add_argument(
        "--artifact-store",
        default=None,
        help=(
            "Artifact store target. Supports paths, file:/..., memory://name, "
            "or s3://bucket/prefix. Defaults to FALA_ARTIFACT_STORE."
        ),
    )
    serve.add_argument(
        "--artifact-store-root",
        default=None,
        help=(
            "Local artifact blob store root. Compatibility alias for "
            "--artifact-store."
        ),
    )
    serve.add_argument(
        "--project-dir",
        default=None,
        help="Fala project root with fala-project.yaml. Defaults to current directory.",
    )
    serve.add_argument(
        "--project-yaml",
        default=None,
        help="Fala project manifest path. Defaults to PROJECT_DIR/fala-project.yaml.",
    )
    serve.add_argument("--title", default="Fala", help="Web page title.")
    serve.add_argument(
        "--log-level",
        default="info",
        choices=("critical", "error", "warning", "info", "debug", "trace"),
        help="Uvicorn log level.",
    )

    worker_commands = subparsers.add_parser(
        "worker-commands",
        help="Render process-runtime-worker commands declared by workflow packages.",
    )
    worker_commands.add_argument("--base-url", required=True, help="Control plane base URL.")
    worker_commands.add_argument("--run-id", required=True)
    worker_commands.add_argument("--package-id", default=None)

    worker_deployment = subparsers.add_parser(
        "worker-deployment",
        help="Render Docker Compose or Kubernetes manifests for package workers.",
    )
    worker_deployment.add_argument("--base-url", required=True, help="Control plane base URL.")
    worker_deployment.add_argument("--run-id", required=True)
    worker_deployment.add_argument("--package-id", default=None)
    worker_deployment.add_argument("--worker-id", action="append", default=[])
    worker_deployment.add_argument("--worker-executable", default="process-runtime-worker")
    worker_deployment.add_argument("--lease-seconds", type=float, default=300.0)
    worker_deployment.add_argument("--idle-sleep", type=float, default=2.0)
    worker_deployment.add_argument("--worker-max-steps", type=int, default=1000)
    worker_deployment.add_argument("--worker-max-idle-polls", type=int, default=1)
    worker_deployment.add_argument("--no-worker-forever", action="store_true")
    worker_deployment.add_argument(
        "--format",
        choices=("docker-compose", "kubernetes"),
        required=True,
    )
    worker_deployment.add_argument("--image", default="fala-worker:latest")
    worker_deployment.add_argument("--replicas", type=int, default=1)
    worker_deployment.add_argument("--namespace", default=None)
    worker_deployment.add_argument(
        "--container-pipeline-dir",
        default=None,
        help=(
            "Pipeline directory path as mounted inside the worker image. "
            "Also maps worker cwd values under the host pipeline dir."
        ),
    )
    worker_deployment.add_argument(
        "--container-workdir",
        default=None,
        help="Override worker working directory inside the container.",
    )
    worker_deployment.add_argument(
        "--no-mount-pipeline-dir",
        action="store_true",
        help=(
            "Do not add a Docker Compose bind mount for --container-pipeline-dir. "
            "Use when the image already contains the package tree."
        ),
    )
    worker_deployment.add_argument(
        "--env",
        action="append",
        default=[],
        help="Environment entry as key=value. Repeatable.",
    )

    deployment = subparsers.add_parser(
        "deployment",
        help="Render a runnable Fala control plane plus package workers.",
    )
    deployment.add_argument(
        "--format",
        choices=("docker-compose", "kubernetes"),
        required=True,
    )
    deployment.add_argument(
        "--run-id",
        default=None,
        help="Run id for generated workers. Required unless --no-workers is set.",
    )
    deployment.add_argument(
        "--base-url",
        default=None,
        help="Control plane URL used by workers. Defaults to the generated service URL.",
    )
    deployment.add_argument("--package-id", default=None)
    deployment.add_argument("--worker-id", action="append", default=[])
    deployment.add_argument("--worker-executable", default="process-runtime-worker")
    deployment.add_argument("--lease-seconds", type=float, default=300.0)
    deployment.add_argument("--idle-sleep", type=float, default=2.0)
    deployment.add_argument("--worker-max-steps", type=int, default=1000)
    deployment.add_argument("--worker-max-idle-polls", type=int, default=1)
    deployment.add_argument("--no-worker-forever", action="store_true")
    deployment.add_argument(
        "--no-workers",
        action="store_true",
        help="Render only the control plane and shared services.",
    )
    deployment.add_argument("--image", default="fala:latest")
    deployment.add_argument(
        "--worker-image",
        default=None,
        help="Worker image. Defaults to --image.",
    )
    deployment.add_argument("--host-port", type=int, default=8000)
    deployment.add_argument("--container-port", type=int, default=8000)
    deployment.add_argument("--control-plane-replicas", type=int, default=1)
    deployment.add_argument("--worker-replicas", type=int, default=1)
    deployment.add_argument("--namespace", default=None)
    deployment.add_argument(
        "--container-pipeline-dir",
        default="/app/pipelines",
        help=(
            "Pipeline directory path mounted inside generated services. "
            "Also maps worker cwd values under the host pipeline dir."
        ),
    )
    deployment.add_argument(
        "--container-workdir",
        default=None,
        help="Override worker working directory inside worker containers.",
    )
    deployment.add_argument(
        "--no-mount-pipeline-dir",
        action="store_true",
        help=(
            "Do not add a Docker Compose bind mount for --container-pipeline-dir. "
            "Use when the image already contains the package tree."
        ),
    )
    deployment.add_argument(
        "--database-url",
        default=None,
        help="FALA_DATABASE_URL for the control plane.",
    )
    deployment.add_argument(
        "--sqlite-db",
        default="/data/fala.db",
        help="SQLite DB path inside the control-plane container.",
    )
    deployment.add_argument(
        "--queue-broker",
        default=None,
        help=(
            "Queue broker target inside the control-plane container. Supports "
            "memory://name, redis://..., sqlite://path, or plain SQLite paths."
        ),
    )
    deployment.add_argument(
        "--queue-db",
        default="/data/queue.sqlite",
        help=(
            "SQLite queue broker path inside the control-plane container. "
            "Compatibility alias for --queue-broker."
        ),
    )
    deployment.add_argument(
        "--artifact-store",
        default=None,
        help=(
            "Artifact store target inside generated services. Supports paths, "
            "file:/..., memory://name, or s3://bucket/prefix."
        ),
    )
    deployment.add_argument(
        "--artifact-store-root",
        default="/data/artifact-store",
        help="Local artifact blob store root inside generated services.",
    )
    deployment.add_argument(
        "--artifact-cache-root",
        default="/data/artifact-cache",
        help="Worker cache root for remote artifact store targets.",
    )
    deployment.add_argument(
        "--process-artifact-root",
        default="/data/process-artifacts",
        help="Worker process artifact root inside generated services.",
    )
    deployment.add_argument("--data-volume", default="fala-data")
    deployment.add_argument(
        "--env",
        action="append",
        default=[],
        help="Control-plane environment entry as key=value. Repeatable.",
    )
    deployment.add_argument(
        "--worker-env",
        action="append",
        default=[],
        help="Worker environment entry as key=value. Repeatable.",
    )

    worker_autoscaling = subparsers.add_parser(
        "worker-autoscaling",
        help="Render KEDA autoscaling manifests for package workers.",
    )
    worker_autoscaling.add_argument("--base-url", required=True, help="Control plane base URL.")
    worker_autoscaling.add_argument("--run-id", required=True)
    worker_autoscaling.add_argument("--package-id", default=None)
    worker_autoscaling.add_argument("--worker-id", action="append", default=[])
    worker_autoscaling.add_argument("--worker-executable", default="process-runtime-worker")
    worker_autoscaling.add_argument("--lease-seconds", type=float, default=300.0)
    worker_autoscaling.add_argument("--idle-sleep", type=float, default=2.0)
    worker_autoscaling.add_argument("--worker-max-steps", type=int, default=1000)
    worker_autoscaling.add_argument("--worker-max-idle-polls", type=int, default=1)
    worker_autoscaling.add_argument("--no-worker-forever", action="store_true")
    worker_autoscaling.add_argument("--prometheus-server", required=True)
    worker_autoscaling.add_argument("--min-replicas", type=int, default=0)
    worker_autoscaling.add_argument("--max-replicas", type=int, default=10)
    worker_autoscaling.add_argument("--target-value", type=int, default=1)
    worker_autoscaling.add_argument("--namespace", default=None)

    queue_export = subparsers.add_parser(
        "queue-export-claims",
        help="Claim queued work through the API and emit QueueWorkEnvelope JSONL.",
    )
    queue_export.add_argument("--base-url", required=True, help="Control plane base URL.")
    queue_export.add_argument("--api-key", default=None, help="Defaults to FALA_API_KEY.")
    queue_export.add_argument("--run-id", required=True)
    queue_export.add_argument("--pipeline", required=True)
    queue_export.add_argument("--worker-id", required=True)
    queue_export.add_argument(
        "--unassigned-claim",
        action="store_true",
        help=(
            "Claim queue work without a control-plane worker owner. Useful when "
            "this command only publishes work and another broker worker executes it."
        ),
    )
    queue_export.add_argument("--process-id", default=None)
    queue_export.add_argument("--capability", action="append", default=[])
    queue_export.add_argument("--queue", default=None)
    queue_export.add_argument("--lease-seconds", type=float, default=300.0)
    queue_export.add_argument("--max-claims", type=int, default=1)
    queue_export.add_argument(
        "--work-file",
        default="-",
        help=(
            "Output JSONL path, or '-' for stdout. Ignored when --queue-broker "
            "or --queue-db is set."
        ),
    )
    queue_export.add_argument(
        "--queue-broker",
        default=None,
        help=(
            "Queue broker target. Supports memory://name, redis://..., "
            "sqlite://path, or plain SQLite paths."
        ),
    )
    queue_export.add_argument(
        "--queue-db",
        default=None,
        help="SQLite queue DB path. Compatibility alias for --queue-broker.",
    )
    _add_resource_args(queue_export)

    queue_run = subparsers.add_parser(
        "queue-run-work",
        help="Run QueueWorkEnvelope JSONL locally and emit QueueResultEnvelope JSONL.",
    )
    queue_run.add_argument(
        "--work-file",
        default="-",
        help=(
            "Input QueueWorkEnvelope JSONL path, or '-' for stdin. Ignored "
            "when --queue-broker or --queue-db is set."
        ),
    )
    queue_run.add_argument(
        "--result-file",
        default="-",
        help=(
            "Output QueueResultEnvelope JSONL path, or '-' for stdout. Ignored "
            "when --queue-broker or --queue-db is set."
        ),
    )
    queue_run.add_argument(
        "--queue-broker",
        default=None,
        help=(
            "Queue broker target. Supports memory://name, redis://..., "
            "sqlite://path, or plain SQLite paths."
        ),
    )
    queue_run.add_argument(
        "--queue-db",
        default=None,
        help="SQLite queue DB path. Compatibility alias for --queue-broker.",
    )
    queue_run.add_argument(
        "--queue",
        default=None,
        help="Queue name to claim from when --queue-broker or --queue-db is set.",
    )
    queue_run.add_argument(
        "--worker-id",
        default=None,
        help=(
            "Broker lease owner recorded when --queue-broker or --queue-db is set."
        ),
    )
    queue_run.add_argument("--lease-seconds", type=float, default=300.0)
    queue_run.add_argument("--max-claims", type=int, default=1)
    queue_run.add_argument(
        "--max-deliveries",
        type=int,
        default=None,
        help=(
            "Broker max delivery attempts before work moves to dead_letter. "
            "Defaults to unlimited."
        ),
    )
    queue_run.add_argument(
        "--base-url",
        default=None,
        help="Control plane base URL used only with --renew-claim.",
    )
    queue_run.add_argument("--api-key", default=None, help="Defaults to FALA_API_KEY.")
    queue_run.add_argument(
        "--renew-claim",
        action="store_true",
        help="Renew the control-plane claim while the worker command runs.",
    )
    queue_run.add_argument(
        "--renew-interval-seconds",
        type=float,
        default=None,
        help="Control-plane claim renewal interval. Defaults to half --lease-seconds, capped.",
    )
    queue_run.add_argument("--error-kind", default="worker_error")
    queue_run.add_argument("--cwd", default=None)
    queue_run.add_argument("--env", action="append", default=[])
    queue_run.add_argument("--timeout-seconds", type=float, default=None)
    queue_run.add_argument(
        "--command",
        dest="exec_command",
        nargs=argparse.REMAINDER,
        default=None,
        help=(
            "Worker-local command. Must be the final option. "
            "Receives ProcessExecutionContext JSON on stdin and emits ProcessOutput JSON."
        ),
    )

    queue_list_work = subparsers.add_parser(
        "queue-list-work",
        help="List broker work rows, including dead-letter work.",
    )
    queue_list_work.add_argument(
        "--queue-broker",
        default=None,
        help=(
            "Queue broker target. Supports memory://name, redis://..., "
            "sqlite://path, or plain SQLite paths."
        ),
    )
    queue_list_work.add_argument(
        "--queue-db",
        default=None,
        help="SQLite queue DB path. Compatibility alias for --queue-broker.",
    )
    queue_list_work.add_argument("--queue", default=None, help="Queue name filter.")
    queue_list_work.add_argument("--state", default=None, help="Work state filter.")
    queue_list_work.add_argument("--limit", type=int, default=None)
    queue_list_work.add_argument(
        "--include-payload",
        action="store_true",
        help="Include QueueWorkEnvelope payloads in output.",
    )

    queue_requeue_work = subparsers.add_parser(
        "queue-requeue-work",
        help="Move a broker work row back to ready.",
    )
    queue_requeue_work.add_argument(
        "--queue-broker",
        default=None,
        help=(
            "Queue broker target. Supports memory://name, redis://..., "
            "sqlite://path, or plain SQLite paths."
        ),
    )
    queue_requeue_work.add_argument(
        "--queue-db",
        default=None,
        help="SQLite queue DB path. Compatibility alias for --queue-broker.",
    )
    queue_requeue_work.add_argument("--work-id", required=True)
    queue_requeue_work.add_argument(
        "--keep-delivery-count",
        action="store_true",
        help="Do not reset delivery_count when requeueing.",
    )

    queue_apply = subparsers.add_parser(
        "queue-apply-results",
        help="Apply QueueResultEnvelope JSONL back to the control plane API.",
    )
    queue_apply.add_argument("--base-url", required=True, help="Control plane base URL.")
    queue_apply.add_argument("--api-key", default=None, help="Defaults to FALA_API_KEY.")
    queue_apply.add_argument(
        "--result-file",
        default="-",
        help=(
            "Input QueueResultEnvelope JSONL path, or '-' for stdin. Ignored "
            "when --queue-broker or --queue-db is set."
        ),
    )
    queue_apply.add_argument(
        "--queue-broker",
        default=None,
        help=(
            "Queue broker target. Supports memory://name, redis://..., "
            "sqlite://path, or plain SQLite paths."
        ),
    )
    queue_apply.add_argument(
        "--queue-db",
        default=None,
        help="SQLite queue DB path. Compatibility alias for --queue-broker.",
    )
    queue_apply.add_argument(
        "--queue",
        default=None,
        help=(
            "Queue name to apply results from when --queue-broker or --queue-db "
            "is set."
        ),
    )
    queue_apply.add_argument("--max-results", type=int, default=None)

    supervise = subparsers.add_parser(
        "supervise-workers",
        help="Run and supervise process-runtime-worker processes from workflow packages.",
    )
    supervise.add_argument("--base-url", required=True, help="Control plane base URL.")
    supervise.add_argument("--run-id", required=True)
    supervise.add_argument("--package-id", default=None)
    supervise.add_argument("--worker-id", action="append", default=[])
    supervise.add_argument("--worker-executable", default="process-runtime-worker")
    supervise.add_argument("--lease-seconds", type=float, default=300.0)
    supervise.add_argument("--idle-sleep", type=float, default=2.0)
    supervise.add_argument("--worker-max-steps", type=int, default=1000)
    supervise.add_argument("--worker-max-idle-polls", type=int, default=1)
    supervise.add_argument("--no-worker-forever", action="store_true")
    supervise.add_argument(
        "--restart-policy",
        choices=("never", "on-failure", "always"),
        default="on-failure",
    )
    supervise.add_argument("--max-restarts", type=int, default=5)
    supervise.add_argument("--restart-delay-seconds", type=float, default=1.0)
    supervise.add_argument("--stop-timeout-seconds", type=float, default=10.0)
    supervise.add_argument("--max-runtime-seconds", type=float, default=None)
    supervise.add_argument("--dry-run", action="store_true")

    describe = subparsers.add_parser("describe", help="Describe one pipeline.")
    describe.add_argument("pipeline_id")

    carriers = subparsers.add_parser(
        "carriers",
        help="Inspect Carrier-first runtime carriers.",
    )
    carrier_subparsers = carriers.add_subparsers(dest="carrier_command", required=True)
    carriers_list = carrier_subparsers.add_parser("list", help="List carriers.")
    _add_carrier_runtime_db_run_args(carriers_list)
    carriers_list.add_argument("--carrier-type", default=None)
    carriers_list.add_argument("--limit", type=int, default=None)
    carriers_list.add_argument("--jsonl", action="store_true")
    carriers_inspect = carrier_subparsers.add_parser("inspect", help="Inspect one carrier.")
    _add_carrier_runtime_db_run_args(carriers_inspect)
    carriers_inspect.add_argument("--carrier-id", required=True)

    carrier_types = subparsers.add_parser(
        "carrier-types",
        help="Inspect Carrier-first runtime carrier types.",
    )
    carrier_type_subparsers = carrier_types.add_subparsers(
        dest="carrier_type_command",
        required=True,
    )
    carrier_types_list = carrier_type_subparsers.add_parser(
        "list",
        help="List carrier types.",
    )
    _add_carrier_runtime_db_run_args(carrier_types_list)
    carrier_types_list.add_argument("--jsonl", action="store_true")
    carrier_types_inspect = carrier_type_subparsers.add_parser(
        "inspect",
        help="Inspect one carrier type.",
    )
    _add_carrier_runtime_db_run_args(carrier_types_inspect)
    carrier_types_inspect.add_argument("--carrier-type-id", required=True)

    carrier_relations = subparsers.add_parser(
        "carrier-relations",
        help="Inspect Carrier-first runtime carrier relations.",
    )
    carrier_relation_subparsers = carrier_relations.add_subparsers(
        dest="carrier_relation_command",
        required=True,
    )
    carrier_relations_list = carrier_relation_subparsers.add_parser(
        "list",
        help="List carrier relations.",
    )
    _add_carrier_runtime_db_run_args(carrier_relations_list)
    carrier_relations_list.add_argument("--carrier-id", default=None)
    carrier_relations_list.add_argument("--relation-type", default=None)
    carrier_relations_list.add_argument("--jsonl", action="store_true")
    carrier_relations_inspect = carrier_relation_subparsers.add_parser(
        "inspect",
        help="Inspect one carrier relation.",
    )
    _add_carrier_runtime_db_run_args(carrier_relations_inspect)
    carrier_relations_inspect.add_argument("--relation-id", required=True)

    observations = subparsers.add_parser(
        "observations",
        help="Inspect Carrier-first runtime observations.",
    )
    observation_subparsers = observations.add_subparsers(
        dest="observation_command",
        required=True,
    )
    observations_list = observation_subparsers.add_parser(
        "list",
        help="List observations.",
    )
    _add_carrier_runtime_db_run_args(observations_list)
    observations_list.add_argument("--carrier-id", default=None)
    observations_list.add_argument("--jsonl", action="store_true")

    events = subparsers.add_parser(
        "events",
        help="Inspect Carrier-first runtime events.",
    )
    event_subparsers = events.add_subparsers(dest="event_command", required=True)
    events_list = event_subparsers.add_parser("list", help="List ordered events.")
    _add_carrier_runtime_db_run_args(events_list)
    events_list.add_argument("--carrier-id", default=None)
    events_list.add_argument("--after-sequence", type=int, default=None)
    events_list.add_argument("--limit", type=int, default=None)
    events_list.add_argument("--jsonl", action="store_true")

    gates = subparsers.add_parser(
        "gates",
        help="Inspect Carrier-first runtime gates.",
    )
    gate_subparsers = gates.add_subparsers(dest="gate_command", required=True)
    gates_list = gate_subparsers.add_parser("list", help="List gates.")
    _add_carrier_runtime_db_run_args(gates_list)
    gates_list.add_argument("--carrier-id", default=None)
    gates_list.add_argument(
        "--status",
        choices=[status.value for status in CarrierGateStatus],
        default=None,
    )
    gates_list.add_argument("--jsonl", action="store_true")

    projections = subparsers.add_parser(
        "projections",
        help="Inspect Carrier-first runtime projections.",
    )
    projection_subparsers = projections.add_subparsers(
        dest="projection_command",
        required=True,
    )
    projections_list = projection_subparsers.add_parser(
        "list",
        help="List projections.",
    )
    _add_carrier_runtime_db_run_args(projections_list)
    projections_list.add_argument("--jsonl", action="store_true")

    init = subparsers.add_parser("init-document", help="Initialize document graph in runtime store.")
    init.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    init.add_argument("--pipeline", required=True, help="Pipeline id.")
    init.add_argument("--run-id", required=True)
    init.add_argument("--document-id", required=True)
    init.add_argument(
        "--value",
        action="append",
        default=[],
        help="Initial value as key=value. Repeatable.",
    )
    init.add_argument(
        "--artifact",
        action="append",
        default=[],
        help="Artifact as kind=uri. Repeatable.",
    )

    create_run = subparsers.add_parser(
        "create-run",
        help="Create a run and initialize many documents in runtime store.",
    )
    create_run.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    create_run.add_argument(
        "--pipeline",
        default=None,
        help="Default pipeline id. Optional when --run-input provides pipeline_id.",
    )
    create_run.add_argument(
        "--run-input",
        default=None,
        help="RuntimeRunInput JSON/YAML manifest path, or '-' for stdin.",
    )
    create_run.add_argument("--run-id", default=None)
    create_run.add_argument(
        "--existing-run",
        choices=("error", "resume"),
        default=None,
        help="Policy when --run-id already exists.",
    )
    create_run.add_argument(
        "--existing-document",
        choices=("error", "reuse"),
        default=None,
        help="Policy when a document_id already exists in the run.",
    )
    create_run.add_argument("--title", default=None)
    create_run.add_argument("--document-type", default=None)
    create_run.add_argument("--media-type", default=None)
    create_run.add_argument(
        "--file",
        action="append",
        default=[],
        help="Local source file. Repeatable.",
    )
    create_run.add_argument(
        "--document",
        action="append",
        default=[],
        help="Document source as document_id=uri. Repeatable.",
    )
    create_run.add_argument(
        "--value",
        action="append",
        default=[],
        help="Initial value copied to each document as key=value. Repeatable.",
    )
    create_run.add_argument(
        "--metadata",
        action="append",
        default=[],
        help="Document metadata copied to each document as key=value. Repeatable.",
    )
    create_run.add_argument(
        "--resource-pool",
        action="append",
        default=[],
        help=(
            "Run resource quota as POOL.KEY=VALUE. Keys: cpu_cores, memory_mb, "
            "disk_mb, gpu_count, units.NAME. Repeatable."
        ),
    )

    create_project_run = subparsers.add_parser(
        "create-project-run",
        help="Create a mixed run from fala-project.yaml source-list routing.",
    )
    create_project_run.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    create_project_run.add_argument(
        "--project-dir",
        default=".",
        help="Project root. Defaults to current directory.",
    )
    create_project_run.add_argument(
        "--project-yaml",
        default=None,
        help="Project manifest path. Defaults to PROJECT_DIR/fala-project.yaml.",
    )
    create_project_run.add_argument("--run-id", default=None)
    create_project_run.add_argument("--title", default=None)
    create_project_run.add_argument(
        "--existing-run",
        choices=("error", "resume"),
        default="error",
        help="Policy when --run-id already exists.",
    )
    create_project_run.add_argument(
        "--existing-document",
        choices=("error", "reuse"),
        default="error",
        help="Policy when a document_id already exists in the run.",
    )
    create_project_run.add_argument(
        "--metadata",
        action="append",
        default=[],
        help="Run metadata as key=value. Repeatable.",
    )

    append_documents = subparsers.add_parser(
        "append-documents",
        help="Append many documents to an existing runtime run.",
    )
    append_documents.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    append_documents.add_argument("--run-id", required=True)
    append_documents.add_argument(
        "--pipeline",
        default=None,
        help="Default pipeline id. Optional when --run-input provides pipeline_id.",
    )
    append_documents.add_argument(
        "--run-input",
        default=None,
        help="RuntimeRunInput JSON/YAML manifest path, or '-' for stdin.",
    )
    append_documents.add_argument(
        "--existing-document",
        choices=("error", "reuse"),
        default=None,
        help="Policy when a document_id already exists in the run.",
    )
    append_documents.add_argument("--document-type", default=None)
    append_documents.add_argument("--media-type", default=None)
    append_documents.add_argument(
        "--file",
        action="append",
        default=[],
        help="Local source file. Repeatable.",
    )
    append_documents.add_argument(
        "--document",
        action="append",
        default=[],
        help="Document source as document_id=uri. Repeatable.",
    )
    append_documents.add_argument(
        "--value",
        action="append",
        default=[],
        help="Initial value copied to each document as key=value. Repeatable.",
    )
    append_documents.add_argument(
        "--metadata",
        action="append",
        default=[],
        help="Document metadata copied to each document as key=value. Repeatable.",
    )
    append_documents.set_defaults(
        existing_run=None,
        title=None,
        resource_pool=[],
    )

    list_documents = subparsers.add_parser(
        "list-documents",
        help="List runtime document registry records from runtime store.",
    )
    list_documents.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    list_documents.add_argument("--run-id", required=True)
    list_documents.add_argument(
        "--status",
        choices=[status.value for status in RuntimeDocumentStatus],
        default=None,
    )
    list_documents.add_argument("--pipeline", default=None)
    list_documents.add_argument("--document-type", default=None)
    list_documents.add_argument("--relation", default=None)
    list_documents.add_argument("--parent-document-id", default=None)
    list_documents.add_argument("--limit", type=int, default=100)
    list_documents.add_argument("--offset", type=int, default=0)
    list_documents.add_argument(
        "--jsonl",
        action="store_true",
        help="Emit one document per line instead of an envelope JSON object.",
    )

    list_processes = subparsers.add_parser(
        "list-processes",
        help="List runtime process records from runtime store.",
    )
    list_processes.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    list_processes.add_argument("--run-id", required=True)
    list_processes.add_argument(
        "--status",
        choices=[status.value for status in ProcessStatus],
        default=None,
    )
    list_processes.add_argument("--pipeline", default=None)
    list_processes.add_argument("--document-type", default=None)
    list_processes.add_argument("--parent-document-id", default=None)
    list_processes.add_argument("--document-id", default=None)
    list_processes.add_argument("--process-id", default=None)
    list_processes.add_argument("--capability", default=None)
    list_processes.add_argument("--operation-type", default=None)
    list_processes.add_argument("--adapter-kind", choices=ADAPTER_KIND_CHOICES, default=None)
    list_processes.add_argument("--resource-pool", default=None)
    list_processes.add_argument("--limit", type=int, default=100)
    list_processes.add_argument("--offset", type=int, default=0)
    list_processes.add_argument(
        "--jsonl",
        action="store_true",
        help="Emit one process per line instead of an envelope JSON object.",
    )

    dead_letter = subparsers.add_parser(
        "dead-letter",
        help="List failed runtime processes requiring operator replay or triage.",
    )
    dead_letter.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    dead_letter.add_argument("--run-id", required=True)
    dead_letter.add_argument("--pipeline", default=None)
    dead_letter.add_argument("--document-type", default=None)
    dead_letter.add_argument("--parent-document-id", default=None)
    dead_letter.add_argument("--document-id", default=None)
    dead_letter.add_argument("--process-id", default=None)
    dead_letter.add_argument("--capability", default=None)
    dead_letter.add_argument("--operation-type", default=None)
    dead_letter.add_argument("--adapter-kind", choices=ADAPTER_KIND_CHOICES, default=None)
    dead_letter.add_argument("--resource-pool", default=None)
    dead_letter.add_argument("--limit", type=int, default=100)
    dead_letter.add_argument("--offset", type=int, default=0)
    dead_letter.add_argument(
        "--jsonl",
        action="store_true",
        help="Emit one dead-letter item per line instead of an envelope JSON object.",
    )

    stuck_work = subparsers.add_parser(
        "stuck-work",
        help="List queued, waiting, or running runtime processes that exceeded SLA thresholds.",
    )
    stuck_work.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    stuck_work.add_argument("--run-id", required=True)
    stuck_work.add_argument(
        "--status",
        choices=[
            ProcessStatus.waiting.value,
            ProcessStatus.queued.value,
            ProcessStatus.running.value,
        ],
        default=None,
    )
    stuck_work.add_argument("--pipeline", default=None)
    stuck_work.add_argument("--document-type", default=None)
    stuck_work.add_argument("--parent-document-id", default=None)
    stuck_work.add_argument("--document-id", default=None)
    stuck_work.add_argument("--process-id", default=None)
    stuck_work.add_argument("--capability", default=None)
    stuck_work.add_argument("--operation-type", default=None)
    stuck_work.add_argument("--adapter-kind", choices=ADAPTER_KIND_CHOICES, default=None)
    stuck_work.add_argument("--resource-pool", default=None)
    stuck_work.add_argument("--waiting-after-seconds", type=float, default=3600.0)
    stuck_work.add_argument("--queued-after-seconds", type=float, default=600.0)
    stuck_work.add_argument("--running-after-seconds", type=float, default=1800.0)
    stuck_work.add_argument("--limit", type=int, default=100)
    stuck_work.add_argument("--offset", type=int, default=0)
    stuck_work.add_argument(
        "--jsonl",
        action="store_true",
        help="Emit one stuck-work item per line instead of an envelope JSON object.",
    )

    diagnose_waits = subparsers.add_parser(
        "diagnose-waits",
        help="Diagnose waiting runtime processes and wait-graph deadlocks.",
    )
    diagnose_waits.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    diagnose_waits.add_argument("--run-id", required=True)
    diagnose_waits.add_argument("--document-id", required=True)
    diagnose_waits.add_argument("--pipeline", default=None)

    stream_lag = subparsers.add_parser(
        "stream-lag",
        help="List process stream consumer lag for one runtime run.",
    )
    stream_lag.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    stream_lag.add_argument("--run-id", required=True)
    stream_lag.add_argument("--pipeline", default=None)
    stream_lag.add_argument("--document-type", default=None)
    stream_lag.add_argument("--parent-document-id", default=None)
    stream_lag.add_argument("--document-id", default=None)
    stream_lag.add_argument("--process-id", default=None)
    stream_lag.add_argument("--capability", default=None)
    stream_lag.add_argument("--operation-type", default=None)
    stream_lag.add_argument("--adapter-kind", choices=ADAPTER_KIND_CHOICES, default=None)
    stream_lag.add_argument("--resource-pool", default=None)
    stream_lag.add_argument("--stream-id", default=None)
    stream_lag.add_argument("--consumer-id", default=None)
    stream_lag.add_argument("--min-lag", type=int, default=1)
    stream_lag.add_argument(
        "--over-limit",
        action="store_true",
        help="Only include stream consumers over their max_buffered_chunks limit.",
    )
    stream_lag.add_argument("--limit", type=int, default=100)
    stream_lag.add_argument("--offset", type=int, default=0)
    stream_lag.add_argument(
        "--jsonl",
        action="store_true",
        help="Emit one stream lag item per line instead of an envelope JSON object.",
    )

    replay_dead_letter = subparsers.add_parser(
        "replay-dead-letter",
        help="Retry one failed runtime process from the dead-letter queue.",
    )
    replay_dead_letter.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    replay_dead_letter.add_argument("--run-id", required=True)
    replay_dead_letter.add_argument("--document-id", required=True)
    replay_dead_letter.add_argument("--process-id", required=True)
    replay_dead_letter.add_argument("--pipeline", default=None)
    replay_dead_letter.add_argument("--reason", default="dead letter replay")
    replay_dead_letter.add_argument(
        "--allow-contract-drift",
        action="store_true",
        help="Allow replay when stored run contracts differ from current registry.",
    )

    control_run = subparsers.add_parser(
        "control-run",
        help="Pause, resume, or cancel a runtime run.",
    )
    control_run.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    control_run.add_argument("--run-id", required=True)
    control_run.add_argument(
        "--action",
        choices=("pause", "resume", "cancel"),
        required=True,
    )
    control_run.add_argument("--reason", default=None)
    control_run.add_argument(
        "--allow-contract-drift",
        action="store_true",
        help="Allow resume when stored run contracts differ from current registry.",
    )

    validate_run = subparsers.add_parser(
        "validate-run",
        help="Validate run input against typed contracts without creating a run.",
    )
    validate_run.add_argument(
        "--pipeline",
        default=None,
        help="Default pipeline id. Optional when --run-input provides pipeline_id.",
    )
    validate_run.add_argument(
        "--run-input",
        default=None,
        help="RuntimeRunInput JSON/YAML manifest path, or '-' for stdin.",
    )
    validate_run.add_argument("--run-id", default=None)
    validate_run.add_argument(
        "--existing-run",
        choices=("error", "resume"),
        default=None,
        help="Policy when --run-id already exists.",
    )
    validate_run.add_argument(
        "--existing-document",
        choices=("error", "reuse"),
        default=None,
        help="Policy when a document_id already exists in the run.",
    )
    validate_run.add_argument("--title", default=None)
    validate_run.add_argument("--document-type", default=None)
    validate_run.add_argument("--media-type", default=None)
    validate_run.add_argument(
        "--file",
        action="append",
        default=[],
        help="Local source file. Repeatable.",
    )
    validate_run.add_argument(
        "--document",
        action="append",
        default=[],
        help="Document source as document_id=uri. Repeatable.",
    )
    validate_run.add_argument(
        "--value",
        action="append",
        default=[],
        help="Initial value copied to each document as key=value. Repeatable.",
    )
    validate_run.add_argument(
        "--metadata",
        action="append",
        default=[],
        help="Document metadata copied to each document as key=value. Repeatable.",
    )
    validate_run.add_argument(
        "--resource-pool",
        action="append",
        default=[],
        help=(
            "Run resource quota as POOL.KEY=VALUE. Keys: cpu_cores, memory_mb, "
            "disk_mb, gpu_count, units.NAME. Repeatable."
        ),
    )

    plan_run = subparsers.add_parser(
        "plan-run",
        help="Plan run input against typed contracts without creating a run.",
    )
    plan_run.add_argument(
        "--pipeline",
        default=None,
        help="Default pipeline id. Optional when --run-input provides pipeline_id.",
    )
    plan_run.add_argument(
        "--run-input",
        default=None,
        help="RuntimeRunInput JSON/YAML manifest path, or '-' for stdin.",
    )
    plan_run.add_argument("--run-id", default=None)
    plan_run.add_argument(
        "--existing-run",
        choices=("error", "resume"),
        default=None,
        help="Policy when --run-id already exists.",
    )
    plan_run.add_argument(
        "--existing-document",
        choices=("error", "reuse"),
        default=None,
        help="Policy when a document_id already exists in the run.",
    )
    plan_run.add_argument("--title", default=None)
    plan_run.add_argument("--document-type", default=None)
    plan_run.add_argument("--media-type", default=None)
    plan_run.add_argument(
        "--file",
        action="append",
        default=[],
        help="Local source file. Repeatable.",
    )
    plan_run.add_argument(
        "--document",
        action="append",
        default=[],
        help="Document source as document_id=uri. Repeatable.",
    )
    plan_run.add_argument(
        "--value",
        action="append",
        default=[],
        help="Initial value copied to each document as key=value. Repeatable.",
    )
    plan_run.add_argument(
        "--metadata",
        action="append",
        default=[],
        help="Document metadata copied to each document as key=value. Repeatable.",
    )
    plan_run.add_argument(
        "--resource-pool",
        action="append",
        default=[],
        help=(
            "Run resource quota as POOL.KEY=VALUE. Keys: cpu_cores, memory_mb, "
            "disk_mb, gpu_count, units.NAME. Repeatable."
        ),
    )

    claim = subparsers.add_parser("claim", help="Claim next ready process from runtime store.")
    claim.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    claim.add_argument("--pipeline", required=True, help="Pipeline id.")
    claim.add_argument("--run-id", required=True)
    claim.add_argument("--worker-id", default=None)
    claim.add_argument("--process-id", default=None)
    claim.add_argument("--adapter-kind", choices=ADAPTER_KIND_CHOICES, default=None)
    claim.add_argument("--capability", action="append", default=[])
    _add_resource_args(claim)
    claim.add_argument("--lease-seconds", type=float, default=300.0)

    work = subparsers.add_parser(
        "work-once",
        help="Claim one ready process, run its adapter, and persist output.",
    )
    work.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    work.add_argument("--pipeline", required=True, help="Pipeline id.")
    work.add_argument("--run-id", required=True)
    work.add_argument("--worker-id", required=True)
    work.add_argument("--process-id", default=None)
    work.add_argument("--adapter-kind", choices=ADAPTER_KIND_CHOICES, default=None)
    work.add_argument("--capability", action="append", default=[])
    _add_resource_args(work)
    work.add_argument("--lease-seconds", type=float, default=300.0)

    run = subparsers.add_parser(
        "run-until-idle",
        help="Run ready processes until no matching claim remains.",
    )
    run.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    run.add_argument("--pipeline", required=True, help="Pipeline id.")
    run.add_argument("--run-id", required=True)
    run.add_argument("--worker-id", required=True)
    run.add_argument("--process-id", default=None)
    run.add_argument("--adapter-kind", choices=ADAPTER_KIND_CHOICES, default=None)
    run.add_argument("--capability", action="append", default=[])
    _add_resource_args(run)
    run.add_argument("--lease-seconds", type=float, default=300.0)
    run.add_argument("--max-steps", type=int, default=1000)
    run.add_argument(
        "--include-events",
        action="store_true",
        help="Include full process event log in returned state.",
    )

    complete = subparsers.add_parser(
        "complete-process",
        help="Write process output and mark the process completed.",
    )
    complete.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    complete.add_argument("--pipeline", default=None, help="Pipeline id.")
    complete.add_argument("--run-id", required=True)
    complete.add_argument("--document-id", required=True)
    complete.add_argument("--process-id", required=True)
    complete.add_argument("--worker-id", default=None)
    complete.add_argument(
        "--output-file",
        default=None,
        help="Full ProcessOutput JSON file path, or '-' for stdin.",
    )
    complete.add_argument("--value", action="append", default=[])
    complete.add_argument("--metadata", action="append", default=[])
    complete.add_argument(
        "--artifact",
        action="append",
        default=[],
        help="Artifact ref as kind=uri. Repeatable.",
    )

    status = subparsers.add_parser("status", help="Show runtime state for a run.")
    status.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    status.add_argument("--run-id", required=True)
    status.add_argument(
        "--include-events",
        action="store_true",
        help="Include full process event log in returned state.",
    )

    queue_metrics = subparsers.add_parser(
        "queue-metrics",
        help="Show queued/running bottlenecks and capacity for one run.",
    )
    queue_metrics.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    queue_metrics.add_argument("--run-id", required=True)

    capability_demands = subparsers.add_parser(
        "capability-demands",
        help="Show worker demand grouped by capability for one run.",
    )
    capability_demands.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    capability_demands.add_argument("--run-id", required=True)
    capability_demands.add_argument("--stale-after-seconds", type=float, default=60.0)

    prometheus_metrics = subparsers.add_parser(
        "metrics-prometheus",
        help="Render Prometheus text metrics for one run.",
    )
    prometheus_metrics.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    prometheus_metrics.add_argument("--run-id", required=True)
    prometheus_metrics.add_argument("--stale-after-seconds", type=float, default=60.0)

    health = subparsers.add_parser(
        "health",
        help="Show aggregated run health issues for one run.",
    )
    health.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    health.add_argument("--run-id", required=True)
    health.add_argument("--stale-after-seconds", type=float, default=60.0)

    worker_health = subparsers.add_parser(
        "worker-health",
        help="Show worker heartbeats and stale/healthy state for one run.",
    )
    worker_health.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    worker_health.add_argument("--run-id", required=True)
    worker_health.add_argument("--stale-after-seconds", type=float, default=60.0)

    audit_log = subparsers.add_parser(
        "audit-log",
        help="Show operator audit events.",
    )
    audit_log.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    audit_log.add_argument("--run-id", default=None)
    audit_log.add_argument("--limit", type=int, default=100)

    trace = subparsers.add_parser(
        "trace",
        help="Show process attempt history for one run.",
    )
    trace.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    trace.add_argument("--run-id", required=True)
    trace.add_argument("--document-id", default=None)
    trace.add_argument("--process-id", default=None)
    trace.add_argument("--operation-type", default=None)

    lineage = subparsers.add_parser(
        "document-lineage",
        help="Show parent/child document graph for one run.",
    )
    lineage.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    lineage.add_argument("--run-id", required=True)

    results = subparsers.add_parser(
        "run-results",
        help="Export process outputs across documents in one run.",
    )
    results.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    results.add_argument("--run-id", required=True)
    results.add_argument("--pipeline", default=None)
    results.add_argument("--process-id", default=None)
    results.add_argument("--document-id", default=None)
    results.add_argument("--document-type", default=None)
    results.add_argument("--operation-type", default=None)
    results.add_argument(
        "--jsonl",
        action="store_true",
        help="Emit one result row per line instead of an envelope JSON object.",
    )

    output_documents = subparsers.add_parser(
        "output-documents",
        help="List typed document products emitted by process outputs.",
    )
    output_documents.add_argument(
        "--db",
        required=True,
        help="Runtime SQLite DB path or sqlite:// URL.",
    )
    output_documents.add_argument("--run-id", required=True)
    output_documents.add_argument("--pipeline", default=None)
    output_documents.add_argument("--process-id", default=None)
    output_documents.add_argument("--document-id", default=None)
    output_documents.add_argument("--source-document-type", default=None)
    output_documents.add_argument("--output-document-id", default=None)
    output_documents.add_argument("--document-type", default=None)
    output_documents.add_argument("--relation", default=None)
    output_documents.add_argument("--media-type", default=None)
    output_documents.add_argument("--limit", type=int, default=100)
    output_documents.add_argument("--offset", type=int, default=0)
    output_documents.add_argument(
        "--jsonl",
        action="store_true",
        help="Emit one output document per line instead of an envelope JSON object.",
    )

    reductions = subparsers.add_parser(
        "run-reductions",
        help="Compute declared run-level reductions from process outputs.",
    )
    reductions.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    reductions.add_argument("--run-id", required=True)
    reductions.add_argument("--pipeline", default=None)
    reductions.add_argument("--reduce-id", default=None)

    artifact_gc = subparsers.add_parser(
        "artifact-gc",
        help="Plan or delete orphaned content-addressed artifact blobs.",
    )
    artifact_gc.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    artifact_gc.add_argument(
        "--artifact-store-root",
        default=None,
        help="Artifact store root. Defaults to FALA_ARTIFACT_STORE_ROOT or .flow-runs/artifact-store.",
    )
    artifact_gc.add_argument(
        "--delete",
        action="store_true",
        help="Delete orphaned blobs. Without this flag, only reports a dry-run plan.",
    )

    retention = subparsers.add_parser(
        "run-retention",
        help="Plan or delete old runtime state for selected run statuses.",
    )
    retention.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    cutoff = retention.add_mutually_exclusive_group(required=True)
    cutoff.add_argument("--before", default=None, help="ISO datetime cutoff.")
    cutoff.add_argument(
        "--older-than-days",
        type=float,
        default=None,
        help="Select runs updated more than N days ago.",
    )
    retention.add_argument(
        "--status",
        action="append",
        default=[],
        choices=[status.value for status in RunStatus],
        help="Run status to purge. Repeatable. Defaults to completed/failed/cancelled.",
    )
    retention.add_argument(
        "--delete",
        action="store_true",
        help="Delete selected runs. Without this flag, only reports a dry-run plan.",
    )

    stream_append = subparsers.add_parser(
        "stream-append",
        help="Append one chunk to a process stream.",
    )
    stream_append.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    stream_append.add_argument("--run-id", required=True)
    stream_append.add_argument("--document-id", required=True)
    stream_append.add_argument("--process-id", required=True)
    stream_append.add_argument("--stream-id", default="main")
    stream_append.add_argument("--sequence", type=int, default=None)
    stream_append.add_argument("--kind", default=None)
    stream_append.add_argument("--value", action="append", default=[])
    stream_append.add_argument("--metadata", action="append", default=[])
    stream_append.add_argument(
        "--artifact",
        action="append",
        default=[],
        help="Artifact ref as kind=uri. Repeatable.",
    )

    stream_list = subparsers.add_parser(
        "stream-list",
        help="List chunks from a process stream.",
    )
    stream_list.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    stream_list.add_argument("--run-id", required=True)
    stream_list.add_argument("--document-id", required=True)
    stream_list.add_argument("--process-id", required=True)
    stream_list.add_argument("--stream-id", default="main")
    stream_list.add_argument("--after-sequence", type=int, default=None)
    stream_list.add_argument("--limit", type=int, default=None)

    stream_checkpoint = subparsers.add_parser(
        "stream-checkpoint",
        help="Store one stream consumer checkpoint.",
    )
    stream_checkpoint.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    stream_checkpoint.add_argument("--run-id", required=True)
    stream_checkpoint.add_argument("--document-id", required=True)
    stream_checkpoint.add_argument("--process-id", required=True)
    stream_checkpoint.add_argument("--stream-id", default="main")
    stream_checkpoint.add_argument("--consumer-id", default="default")
    stream_checkpoint.add_argument("--sequence", type=int, required=True)
    stream_checkpoint.add_argument("--chunk-id", default=None)
    stream_checkpoint.add_argument("--metadata", action="append", default=[])

    stream_checkpoint_get = subparsers.add_parser(
        "stream-checkpoint-get",
        help="Read one stream consumer checkpoint.",
    )
    stream_checkpoint_get.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    stream_checkpoint_get.add_argument("--run-id", required=True)
    stream_checkpoint_get.add_argument("--document-id", required=True)
    stream_checkpoint_get.add_argument("--process-id", required=True)
    stream_checkpoint_get.add_argument("--stream-id", default="main")
    stream_checkpoint_get.add_argument("--consumer-id", default="default")

    return parser


def _serve_runtime_web(args: argparse.Namespace) -> int:
    try:
        from fala.web import create_runtime_web_app
        import uvicorn
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError(
            "`fala serve` requires optional web dependencies; install `fala[web]`."
        ) from exc

    _warn_public_serve_without_auth(args.host)
    app = create_runtime_web_app(
        pipeline_dir=args.pipeline_dir,
        db=args.db,
        artifact_roots=args.artifact_root or None,
        artifact_store=args.artifact_store,
        artifact_store_root=args.artifact_store_root,
        queue_broker=args.queue_broker,
        queue_db=args.queue_db,
        project_dir=args.project_dir,
        project_yaml=args.project_yaml,
        title=args.title,
    )
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level=args.log_level,
    )
    return 0


def _warn_public_serve_without_auth(
    host: str,
    *,
    environ: dict[str, str] | None = None,
    stream: Any | None = None,
) -> None:
    policy = RuntimeAccessPolicy.from_env(environ)
    if policy.auth_required or _is_loopback_bind_host(host):
        return
    output = stream or sys.stderr
    print(
        "warning: fala serve is binding to a non-loopback host without auth; "
        "set FALA_API_KEYS or FALA_AUTH_REQUIRED=1 before exposing it.",
        file=output,
    )


def _is_loopback_bind_host(host: str) -> bool:
    normalized = host.strip().lower().removeprefix("[").removesuffix("]")
    if normalized in {"localhost"}:
        return True
    try:
        return ip_address(normalized).is_loopback
    except ValueError:
        return False


async def _run(args: argparse.Namespace) -> dict[str, Any] | None:
    if args.command == "schema":
        model = CONTRACT_MODELS[args.model]
        return {
            "ok": True,
            "model": args.model,
            "schema": model.model_json_schema(),
        }

    if args.command == "validate-output":
        output = _validate_json_contract(ProcessOutput, args.file)
        return {
            "ok": True,
            "model": "process-output",
            "artifact_count": len(output.artifacts),
            "value_keys": sorted(output.values),
            "metadata_keys": sorted(output.metadata),
        }

    if args.command == "validate-context":
        context = _validate_json_contract(ProcessExecutionContext, args.file)
        return {
            "ok": True,
            "model": "process-context",
            "pipeline_id": context.pipeline_id,
            "run_id": context.run_id,
            "document_id": context.document_id,
            "process_id": context.process_id,
            "attempt": context.attempt,
            "artifact_count": len(context.input.artifacts),
            "initial_keys": sorted(
                (context.input.values.get("initial") or {}).keys()
                if isinstance(context.input.values.get("initial"), dict)
                else []
            ),
            "needs": sorted(
                (context.input.values.get("needs") or {}).keys()
                if isinstance(context.input.values.get("needs"), dict)
                else []
            ),
        }

    if args.command == "run-gates":
        report = run_gate_suite_from_file(
            args.config,
            base_dir=args.base_dir,
            evidence_output=args.evidence_output,
        )
        payload = report.model_dump(mode="json")
        if args.output:
            _write_json_output(args.output, payload)
        return payload

    if args.command == "inspect-run-input":
        return _inspect_runtime_run_input(args.run_input)

    if args.command == "step-replay":
        command = _parse_command(args.exec_command)
        if not command:
            raise ValueError("step-replay requires a command")
        return replay_step_manifest(
            args.manifest,
            command,
            cwd=args.cwd,
            env=_parse_env(args.env),
            timeout_seconds=args.timeout_seconds,
        )

    if args.command == "step-bundle":
        command = _parse_command(args.exec_command)
        if not command:
            raise ValueError("step-bundle requires a command")
        return write_step_replay_bundle(
            args.manifest,
            output=args.output,
            command=command,
            cwd=args.cwd,
            env=_parse_env(args.env),
            bundle_name=args.bundle_name,
        )

    if args.command == "step-bundle-verify":
        return verify_step_replay_bundle(args.bundle)

    if args.command == "scaffold-blueprints":
        if args.blueprint and args.blueprint_file:
            raise ValueError("--blueprint and --blueprint-file are mutually exclusive")
        if args.query and (args.blueprint or args.blueprint_file):
            raise ValueError("--query filters the catalog and cannot be combined with --blueprint or --blueprint-file")
        if args.blueprint_file:
            blueprint = _scaffold_blueprint_from_file(args.blueprint_file)
            summary = scaffold_blueprint_summary(blueprint)
            summary["scaffold_command"] = (
                "uv run fala scaffold --blueprint-file "
                f"{args.blueprint_file} --output-dir ./pipelines/{blueprint.id} "
                f"--package-id {blueprint.id} --pipeline-id {blueprint.id}_flow"
            )
            return {
                "ok": True,
                "blueprint": summary,
                "source": args.blueprint_file,
            }
        blueprints = list_scaffold_blueprints(query=args.query)
        if args.blueprint:
            blueprint = get_scaffold_blueprint(args.blueprint)
            assert blueprint is not None
            return {
                "ok": True,
                "blueprint": scaffold_blueprint_summary(blueprint),
            }
        return {
            "ok": True,
            "query": args.query,
            "blueprint_count": len(blueprints),
            "blueprints": blueprints,
        }

    if args.command == "init-project":
        return _init_project_command(args)

    if args.command == "project-doctor":
        return _project_doctor_command(args)

    if args.command == "project-spec":
        return _project_spec_command(args)

    if args.command == "project-check":
        return _project_check_command(args)

    if args.command == "project-smoke":
        return await _project_smoke_command(args)

    if args.command == "project-secrets":
        return _project_secrets_command(args)

    if args.command == "project-bundle":
        return _project_bundle_command(args)

    if args.command == "project-bundle-verify":
        return verify_project_bundle(args.bundle)

    if args.command == "project-supervision":
        return await _project_supervision_command(args)

    if args.command == "project-operations":
        return await _project_operations_command(args)

    if args.command == "project-alerts":
        return await _project_alerts_command(args)

    if args.command == "project-lifecycle":
        return await _project_lifecycle_command(args)

    if args.command in {
        "carrier-relations",
        "carrier-types",
        "carriers",
        "events",
        "gates",
        "observations",
        "projections",
    }:
        return await _carrier_runtime_command(args)

    if args.command == "create-project-run":
        project_yaml = (
            Path(args.project_yaml).expanduser().resolve()
            if args.project_yaml
            else (Path(args.project_dir).expanduser() / "fala-project.yaml").resolve()
        )
        pipeline_dir = (
            Path(args.pipeline_dir).expanduser().resolve()
            if args.pipeline_dir
            else project_pipeline_dir(project_yaml)
        )
        registry = PipelineRegistry.from_directory(pipeline_dir)
        service = RuntimeService(
            registry=registry,
            store=create_state_store(args.db),
        )
        run_input, route_report = build_project_runtime_run_input(
            project_yaml,
            registry=registry,
            run_id=args.run_id,
            title=args.title,
            existing_run_policy=args.existing_run,
            existing_document_policy=args.existing_document,
            metadata=_parse_values(args.metadata),
        )
        run, schedules = await service.create_run_with_documents(
            run_input,
            route_report=route_report,
        )
        return {
            "ok": True,
            "project_yaml": str(project_yaml),
            "pipeline_dir": str(pipeline_dir),
            "run": run.model_dump(mode="json"),
            "document_count": len(schedules),
            "route_report": route_report,
            "schedules": [schedule.model_dump(mode="json") for schedule in schedules],
        }

    if args.command == "scaffold":
        scaffold_input = _scaffold_input_from_args(args)
        return _scaffold_workflow_package(
            output_dir=Path(args.output_dir),
            package_id=args.package_id,
            pipeline_id=args.pipeline_id,
            steps=scaffold_input["steps"],
            adapter_kind=args.adapter_kind,
            title=args.title,
            document_type=scaffold_input["document_type"],
            document_media_types=scaffold_input["document_media_types"],
            document_extensions=scaffold_input["document_extensions"],
            document_value_schema=scaffold_input["document_value_schema"],
            document_metadata_schema=scaffold_input["document_metadata_schema"],
            additional_document_types=scaffold_input["additional_document_types"],
            additional_document_relations=scaffold_input[
                "additional_document_relations"
            ],
            operation_types=scaffold_input["operation_types"],
            operation_type_by_step=scaffold_input["operation_type_by_step"],
            needs_by_step=scaffold_input["needs_by_step"],
            artifact_kind_by_step=scaffold_input["artifact_kind_by_step"],
            capability_by_step=scaffold_input["capability_by_step"],
            accepted_document_types_by_step=scaffold_input[
                "accepted_document_types_by_step"
            ],
            emitted_document_types_by_step=scaffold_input[
                "emitted_document_types_by_step"
            ],
            artifact_media_types_by_step=scaffold_input["artifact_media_types_by_step"],
            artifact_extensions_by_step=scaffold_input["artifact_extensions_by_step"],
            artifact_value_schema_by_step=scaffold_input[
                "artifact_value_schema_by_step"
            ],
            capability_output_schema_by_step=scaffold_input[
                "capability_output_schema_by_step"
            ],
            capability_streams_by_step=scaffold_input["capability_streams_by_step"],
            step_policy_by_step=scaffold_input["step_policy_by_step"],
            step_guidance_by_step=scaffold_input["step_guidance_by_step"],
            blueprint_id=scaffold_input["blueprint_id"],
        )

    if args.command == "sync-contracts":
        return _sync_contracts_command(args)

    if args.command == "discover-documents":
        registry = (
            PipelineRegistry.from_directory(_pipeline_dir(args))
            if args.auto_route
            else None
        )
        run_input, route_report = _discover_runtime_run_input_with_report(
            args,
            registry=registry,
        )
        if args.route_report:
            _write_json_output(args.route_report, route_report)
        return run_input.model_dump(
            mode="json",
            exclude_none=True,
        )

    if args.command == "queue-export-claims":
        broker_target = _queue_broker_target_from_args(args)
        transport = (
            create_queue_broker_transport(broker_target)
            if broker_target is not None
            else _CollectingQueueTransport()
        )
        ProcessRuntimeClient = _require_process_runtime_client()
        async with ProcessRuntimeClient(
            args.base_url,
            api_key=args.api_key or os.environ.get("FALA_API_KEY"),
        ) as client:
            envelopes = await export_claims_to_queue(
                client,
                transport,
                run_id=args.run_id,
                pipeline_id=args.pipeline,
                worker_id=None if args.unassigned_claim else args.worker_id,
                process_id=args.process_id,
                capabilities=args.capability,
                resources=_parse_resources(args),
                lease_seconds=args.lease_seconds,
                max_claims=args.max_claims,
                queue=args.queue,
                metadata={
                    "source": "fala.queue-export-claims",
                    "publisher_worker_id": args.worker_id,
                    "unassigned_claim": args.unassigned_claim,
                },
            )
        if broker_target is None:
            _write_queue_jsonl(envelopes, args.work_file)
            return None
        return {
            "ok": True,
            "queue_broker": broker_target,
            "queue_db": args.queue_db,
            "exported_count": len(envelopes),
            "work_ids": [envelope.id for envelope in envelopes],
            "stats": await transport.stats(),
        }

    if args.command == "queue-run-work":
        command = _parse_command(args.exec_command)
        if not command:
            raise ValueError("queue-run-work requires --command")
        if args.renew_claim and not args.base_url:
            raise ValueError("queue-run-work --renew-claim requires --base-url")
        adapters = AdapterRegistry.default()
        adapters.register(
            "queue",
            ExternalCommandAdapter(
                command=command,
                cwd=args.cwd,
                env=_parse_env(args.env),
                timeout_seconds=args.timeout_seconds,
            ),
        )
        if args.renew_claim:
            ProcessRuntimeClient = _require_process_runtime_client()
            async with ProcessRuntimeClient(
                args.base_url,
                api_key=args.api_key or os.environ.get("FALA_API_KEY"),
            ) as renew_client:
                return await _run_queue_work_from_args(
                    args,
                    adapters=adapters,
                    renew_client=renew_client,
                )
        return await _run_queue_work_from_args(
            args,
            adapters=adapters,
            renew_client=None,
        )

    if args.command == "queue-list-work":
        broker_target = _require_queue_broker_target(args)
        transport = create_queue_broker_transport(broker_target)
        records: list[SQLiteQueueWorkRecord] = await transport.list_work_records(
            queue=args.queue,
            state=args.state,
            limit=args.limit,
            include_payload=args.include_payload,
        )
        return {
            "ok": True,
            "queue_broker": broker_target,
            "queue_db": args.queue_db,
            "work_count": len(records),
            "work": [
                record.model_dump(mode="json", exclude_none=True)
                for record in records
            ],
            "stats": await transport.stats(),
        }

    if args.command == "queue-requeue-work":
        broker_target = _require_queue_broker_target(args)
        transport = create_queue_broker_transport(broker_target)
        work = await transport.requeue_work(
            args.work_id,
            reset_delivery_count=not args.keep_delivery_count,
        )
        if work is None:
            raise ValueError(f"queue work not found: {args.work_id}")
        return {
            "ok": True,
            "queue_broker": broker_target,
            "queue_db": args.queue_db,
            "work_id": work.id,
            "queue": work.queue,
            "reset_delivery_count": not args.keep_delivery_count,
            "stats": await transport.stats(),
        }

    if args.command == "queue-apply-results":
        broker_target = _queue_broker_target_from_args(args)
        transport = (
            create_queue_broker_transport(broker_target)
            if broker_target is not None
            else None
        )
        results = (
            await transport.load_results(
                queue=args.queue,
                limit=args.max_results,
            )
            if transport is not None
            else read_result_jsonl(_queue_text_source(args.result_file))
        )
        ProcessRuntimeClient = _require_process_runtime_client()
        async with ProcessRuntimeClient(
            args.base_url,
            api_key=args.api_key or os.environ.get("FALA_API_KEY"),
        ) as client:
            applied = await apply_queue_results(client, results)
        if transport is not None:
            for result in results:
                await transport.mark_result_applied(result.id)
        return {
            "ok": True,
            "queue_broker": broker_target,
            "queue_db": args.queue_db,
            "applied_count": len(applied),
            "results": applied,
            "stats": await transport.stats() if transport is not None else None,
        }

    if args.command == "db-doctor":
        report = runtime_db_diagnostics(
            args.db,
            ensure_schema=args.ensure_schema,
        )
        if args.output:
            output = Path(args.output)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(
                json.dumps(report, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            return {
                "ok": report["ok"],
                "output": str(output),
                "store_kind": report["store_kind"],
                "missing_table_count": len(report["schema"]["missing_tables"]),
                "current_version": report["schema"]["current_version"],
                "latest_version": report["schema"]["latest_version"],
                "missing_migration_count": report["schema"]["migrations"][
                    "missing_count"
                ],
            }
        return report

    registry = PipelineRegistry.from_directory(_pipeline_dir(args))

    if args.command == "package-index":
        index = build_workflow_registry_index(
            registry,
            package_id=args.package_id,
        )
        if args.output:
            path = Path(args.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(index.model_dump(mode="json"), indent=2, sort_keys=True)
                + "\n",
                encoding="utf-8",
            )
            return {
                "ok": True,
                "output": str(path),
                "package_count": index.package_count,
                "pipeline_count": index.pipeline_count,
            }
        return {
            "ok": True,
            "index": index.model_dump(mode="json"),
        }

    if args.command == "package-doctor":
        report = build_workflow_readiness_report(
            registry,
            package_id=args.package_id,
            contract_refs=args.contract,
            python_paths=args.python_path,
            discover_contracts=not args.no_discover_contracts,
        )
        if args.output:
            path = Path(args.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(report.model_dump(mode="json"), indent=2, sort_keys=True)
                + "\n",
                encoding="utf-8",
            )
            return {
                "ok": report.ok,
                "output": str(path),
                "package_count": report.package_count,
                "error_count": report.error_count,
                "warning_count": report.warning_count,
                "info_count": report.info_count,
            }
        return {
            "ok": report.ok,
            "readiness": report.model_dump(mode="json"),
        }

    if args.command == "validate":
        pipelines = registry.all()
        if not pipelines:
            raise ValueError(f"No pipelines found in {_pipeline_dir(args)}")
        command_issues = (
            [
                *_validate_subprocess_commands(pipelines),
                *_validate_package_worker_commands(registry),
            ]
            if args.check_commands
            else []
        )
        payload = {
            "ok": not command_issues,
            "package_count": len(registry.packages()),
            "packages": [_package_summary(package) for package in registry.packages()],
            "pipeline_count": len(pipelines),
            "pipelines": [
                _pipeline_summary(pipeline, registry=registry)
                for pipeline in pipelines
            ],
        }
        if args.check_commands:
            payload["command_issues"] = command_issues
        if command_issues and not args.json:
            _print_plain_validation(payload)
            raise ValueError(
                f"{len(command_issues)} subprocess command(s) unavailable"
            )
        return payload if args.json else _print_plain_validation(payload)

    if args.command == "list":
        return {
            "packages": [_package_summary(package) for package in registry.packages()],
            "pipelines": [
                _pipeline_summary(pipeline, registry=registry)
                for pipeline in registry.all()
            ],
        }

    if args.command == "contract":
        return {
            "ok": True,
            "contract": registry.pipeline_contract(args.pipeline_id),
        }

    if args.command == "contract-lint":
        contracts = load_step_contract_refs(
            args.contract,
            python_paths=args.python_path,
        )
        discovery: dict[str, Any] | None = None
        if not args.no_discover_contracts and not args.contract:
            discovery = discover_step_contracts(
                registry,
                pipeline_id=args.pipeline,
                python_paths=args.python_path,
                roots=[_pipeline_dir(args)],
            )
            contracts.extend(discovery["contracts"])
        if not contracts:
            raise ValueError(
                "contract-lint requires --contract or discoverable contracts.py/*_contracts.py"
            )
        report = lint_step_contracts(
            registry,
            pipeline_id=args.pipeline,
            contracts=contracts,
            require_all_steps=not args.allow_missing,
        )
        if discovery is not None:
            report["discovery"] = {
                "refs": discovery["refs"],
                "errors": discovery["errors"],
                "python_paths": discovery["python_paths"],
                "roots": discovery["roots"],
            }
        return report

    if args.command == "worker-commands":
        packages = (
            [registry.package(args.package_id)]
            if args.package_id is not None
            else registry.packages()
        )
        return {
            "ok": True,
            "pipeline_dir": str(_pipeline_dir(args)),
            "base_url": args.base_url,
            "run_id": args.run_id,
            "workers": [
                _worker_command_summary(
                    package_id=package.id,
                    worker=worker,
                    pipeline_dir=_pipeline_dir(args),
                    base_url=args.base_url,
                    run_id=args.run_id,
                )
                for package in packages
                for worker in package.workers
            ],
        }

    if args.command == "worker-deployment":
        specs = build_package_worker_specs(
            registry=registry,
            pipeline_dir=_pipeline_dir(args),
            base_url=args.base_url,
            run_id=args.run_id,
            package_id=args.package_id,
            worker_ids=args.worker_id,
            worker_executable=args.worker_executable,
            worker_forever=not args.no_worker_forever,
            lease_seconds=args.lease_seconds,
            idle_sleep=args.idle_sleep,
            max_steps=args.worker_max_steps,
            max_idle_polls=args.worker_max_idle_polls,
        )
        manifest = render_worker_deployment_manifest(
            specs,
            format=args.format,
            image=args.image,
            replicas=args.replicas,
            namespace=args.namespace,
            env=_parse_values(args.env),
            container_pipeline_dir=args.container_pipeline_dir,
            container_workdir=args.container_workdir,
            mount_pipeline_dir=False if args.no_mount_pipeline_dir else None,
        )
        return {
            "ok": True,
            "format": args.format,
            "worker_count": len(specs),
            "manifest": manifest,
        }

    if args.command == "deployment":
        if args.no_workers:
            specs = []
            base_url = args.base_url or _default_deployment_base_url(
                args.format,
                args.container_port,
            )
        else:
            if not args.run_id:
                raise ValueError("--run-id is required unless --no-workers is set")
            base_url = args.base_url or _default_deployment_base_url(
                args.format,
                args.container_port,
            )
            specs = build_package_worker_specs(
                registry=registry,
                pipeline_dir=_pipeline_dir(args),
                base_url=base_url,
                run_id=args.run_id,
                package_id=args.package_id,
                worker_ids=args.worker_id,
                worker_executable=args.worker_executable,
                worker_forever=not args.no_worker_forever,
                lease_seconds=args.lease_seconds,
                idle_sleep=args.idle_sleep,
                max_steps=args.worker_max_steps,
                max_idle_polls=args.worker_max_idle_polls,
            )
        manifest = render_control_plane_deployment_manifest(
            specs,
            format=args.format,
            image=args.image,
            worker_image=args.worker_image,
            namespace=args.namespace,
            env=_parse_values(args.env),
            worker_env=_parse_values(args.worker_env),
            host_port=args.host_port,
            container_port=args.container_port,
            control_plane_replicas=args.control_plane_replicas,
            worker_replicas=args.worker_replicas,
            pipeline_dir=_pipeline_dir(args),
            container_pipeline_dir=args.container_pipeline_dir,
            container_workdir=args.container_workdir,
            mount_pipeline_dir=False if args.no_mount_pipeline_dir else None,
            database_url=args.database_url,
            sqlite_db=args.sqlite_db,
            queue_broker=args.queue_broker,
            queue_db=args.queue_db,
            artifact_store=args.artifact_store,
            artifact_store_root=args.artifact_store_root,
            artifact_cache_root=args.artifact_cache_root,
            process_artifact_root=args.process_artifact_root,
            data_volume=args.data_volume,
        )
        return {
            "ok": True,
            "format": args.format,
            "base_url": base_url,
            "worker_count": len(specs),
            "manifest": manifest,
        }

    if args.command == "worker-autoscaling":
        specs = build_package_worker_specs(
            registry=registry,
            pipeline_dir=_pipeline_dir(args),
            base_url=args.base_url,
            run_id=args.run_id,
            package_id=args.package_id,
            worker_ids=args.worker_id,
            worker_executable=args.worker_executable,
            worker_forever=not args.no_worker_forever,
            lease_seconds=args.lease_seconds,
            idle_sleep=args.idle_sleep,
            max_steps=args.worker_max_steps,
            max_idle_polls=args.worker_max_idle_polls,
        )
        manifest = render_worker_autoscaling_manifest(
            specs,
            run_id=args.run_id,
            prometheus_server=args.prometheus_server,
            min_replicas=args.min_replicas,
            max_replicas=args.max_replicas,
            target_value=args.target_value,
            namespace=args.namespace,
        )
        return {
            "ok": True,
            "format": "keda",
            "worker_count": len(specs),
            "manifest": manifest,
        }

    if args.command == "supervise-workers":
        specs = build_package_worker_specs(
            registry=registry,
            pipeline_dir=_pipeline_dir(args),
            base_url=args.base_url,
            run_id=args.run_id,
            package_id=args.package_id,
            worker_ids=args.worker_id,
            worker_executable=args.worker_executable,
            worker_forever=not args.no_worker_forever,
            lease_seconds=args.lease_seconds,
            idle_sleep=args.idle_sleep,
            max_steps=args.worker_max_steps,
            max_idle_polls=args.worker_max_idle_polls,
        )
        if args.dry_run:
            return {
                "ok": True,
                "run_id": args.run_id,
                "worker_count": len(specs),
                "workers": [spec.model_dump(mode="json") for spec in specs],
            }
        supervisor = ProcessSupervisor(
            specs,
            restart_policy=args.restart_policy,
            max_restarts=args.max_restarts,
            restart_delay_seconds=args.restart_delay_seconds,
            stop_timeout_seconds=args.stop_timeout_seconds,
        )
        result = await supervisor.run(max_runtime_seconds=args.max_runtime_seconds)
        return result.model_dump(mode="json")

    if args.command == "describe":
        pipeline = registry.get(args.pipeline_id)
        return {"pipeline": pipeline.model_dump(mode="json")}

    if args.command == "init-document":
        pipeline = registry.get(args.pipeline)
        store = create_state_store(args.db)
        scheduler = PipelineScheduler(pipeline, store)
        schedule = await scheduler.initialize_document(
            run_id=args.run_id,
            document_id=args.document_id,
            values=_parse_values(args.value),
            artifacts=_parse_artifacts(args.artifact),
        )
        return {"ok": True, "schedule": schedule.model_dump(mode="json")}

    if args.command == "create-run":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        run_input = _runtime_run_input_from_args(args, command_label="create-run")
        run, schedules = await service.create_run_with_documents(
            run_input
        )
        return {
            "ok": True,
            "run": run.model_dump(mode="json"),
            "document_count": len(schedules),
            "schedules": [schedule.model_dump(mode="json") for schedule in schedules],
        }

    if args.command == "append-documents":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        run_input = _runtime_run_input_from_args(
            args,
            command_label="append-documents",
        )
        if run_input.run_id is None:
            raise ValueError("append-documents requires --run-id")
        run, schedules = await service.append_run_documents(
            run_id=run_input.run_id,
            pipeline_id=run_input.pipeline_id,
            documents=run_input.documents,
            existing_document_policy=run_input.existing_document_policy,
        )
        await service.record_operator_audit(
            actor=os.environ.get("FALA_ACTOR") or os.environ.get("USER") or "cli",
            source="cli",
            action="documents.append",
            run_id=run.id,
            target=f"run:{run.id}/documents",
            data={
                "pipeline_id": run_input.pipeline_id,
                "existing_document_policy": run_input.existing_document_policy,
                "document_count": len(run_input.documents),
                "document_ids": [document.document_id for document in run_input.documents],
                "scheduled_count": len(schedules),
            },
        )
        return {
            "ok": True,
            "run": run.model_dump(mode="json"),
            "document_count": len(schedules),
            "schedules": [schedule.model_dump(mode="json") for schedule in schedules],
        }

    if args.command == "list-documents":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        page = await service.document_registry(
            args.run_id,
            status=RuntimeDocumentStatus(args.status) if args.status else None,
            pipeline_id=args.pipeline,
            document_type=args.document_type,
            relation=args.relation,
            parent_document_id=args.parent_document_id,
            limit=args.limit,
            offset=args.offset,
        )
        if args.jsonl:
            for document in page.documents:
                print(json.dumps(document.model_dump(mode="json"), sort_keys=True))
            return None
        return {
            "ok": True,
            "documents": page.model_dump(mode="json"),
        }

    if args.command == "list-processes":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        page = await service.process_registry(
            args.run_id,
            status=ProcessStatus(args.status) if args.status else None,
            pipeline_id=args.pipeline,
            document_type=args.document_type,
            parent_document_id=args.parent_document_id,
            document_id=args.document_id,
            process_id=args.process_id,
            capability=args.capability,
            operation_type=args.operation_type,
            adapter_kind=args.adapter_kind,
            resource_pool=args.resource_pool,
            limit=args.limit,
            offset=args.offset,
        )
        if args.jsonl:
            for process in page.processes:
                print(json.dumps(process.model_dump(mode="json"), sort_keys=True))
            return None
        return {
            "ok": True,
            "processes": page.model_dump(mode="json"),
        }

    if args.command == "dead-letter":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        page = await service.dead_letter_queue(
            args.run_id,
            pipeline_id=args.pipeline,
            document_type=args.document_type,
            parent_document_id=args.parent_document_id,
            document_id=args.document_id,
            process_id=args.process_id,
            capability=args.capability,
            operation_type=args.operation_type,
            adapter_kind=args.adapter_kind,
            resource_pool=args.resource_pool,
            limit=args.limit,
            offset=args.offset,
        )
        if args.jsonl:
            for item in page.items:
                print(json.dumps(item.model_dump(mode="json"), sort_keys=True))
            return None
        return {
            "ok": True,
            "dead_letter": page.model_dump(mode="json"),
        }

    if args.command == "stuck-work":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        page = await service.stuck_work(
            args.run_id,
            status=ProcessStatus(args.status) if args.status else None,
            pipeline_id=args.pipeline,
            document_type=args.document_type,
            parent_document_id=args.parent_document_id,
            document_id=args.document_id,
            process_id=args.process_id,
            capability=args.capability,
            operation_type=args.operation_type,
            adapter_kind=args.adapter_kind,
            resource_pool=args.resource_pool,
            waiting_after_seconds=args.waiting_after_seconds,
            queued_after_seconds=args.queued_after_seconds,
            running_after_seconds=args.running_after_seconds,
            limit=args.limit,
            offset=args.offset,
        )
        if args.jsonl:
            for item in page.items:
                print(json.dumps(item.model_dump(mode="json"), sort_keys=True))
            return None
        return {
            "ok": True,
            "stuck_work": page.model_dump(mode="json"),
        }

    if args.command == "diagnose-waits":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        diagnostic = await service.diagnose_waits(
            run_id=args.run_id,
            document_id=args.document_id,
            pipeline_id=args.pipeline,
        )
        return {
            "ok": True,
            "wait_diagnostics": diagnostic.model_dump(mode="json"),
        }

    if args.command == "stream-lag":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        page = await service.stream_lag(
            args.run_id,
            pipeline_id=args.pipeline,
            document_type=args.document_type,
            parent_document_id=args.parent_document_id,
            document_id=args.document_id,
            process_id=args.process_id,
            capability=args.capability,
            operation_type=args.operation_type,
            adapter_kind=args.adapter_kind,
            resource_pool=args.resource_pool,
            stream_id=args.stream_id,
            consumer_id=args.consumer_id,
            min_lag=args.min_lag,
            over_limit=True if args.over_limit else None,
            limit=args.limit,
            offset=args.offset,
        )
        if args.jsonl:
            for item in page.items:
                print(json.dumps(item.model_dump(mode="json"), sort_keys=True))
            return None
        return {
            "ok": True,
            "stream_lag": page.model_dump(mode="json"),
        }

    if args.command == "replay-dead-letter":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        result = await service.control_process(
            run_id=args.run_id,
            document_id=args.document_id,
            process_id=args.process_id,
            pipeline_id=args.pipeline,
            action=ProcessAction.retry,
            reason=args.reason,
            allow_contract_drift=args.allow_contract_drift,
        )
        await service.record_operator_audit(
            actor=os.environ.get("FALA_ACTOR") or os.environ.get("USER") or "cli",
            source="cli",
            action="process.dead_letter.replay",
            run_id=args.run_id,
            document_id=args.document_id,
            process_id=args.process_id,
            target=(
                f"run:{args.run_id}/document:{args.document_id}/"
                f"process:{args.process_id}"
            ),
            data={
                "pipeline_id": args.pipeline,
                "reason": args.reason,
                "affected": result.affected,
                "queued_count": len(result.schedule.queued),
                "waiting_count": len(result.schedule.waiting),
                "allow_contract_drift": args.allow_contract_drift,
            },
        )
        return {
            "ok": True,
            "action": result.model_dump(mode="json"),
        }

    if args.command == "validate-run":
        service = RuntimeService(registry=registry, store=InMemoryStateStore())
        return service.preview_runtime_run_input(
            _runtime_run_input_from_args(args, command_label="validate-run")
        )

    if args.command == "plan-run":
        service = RuntimeService(registry=registry, store=InMemoryStateStore())
        return service.plan_runtime_run_input(
            _runtime_run_input_from_args(args, command_label="plan-run")
        )

    if args.command == "claim":
        store = create_state_store(args.db)
        service = RuntimeService(registry=registry, store=store)
        claim = await service.claim_next(
            run_id=args.run_id,
            pipeline_id=args.pipeline,
            worker_id=args.worker_id,
            process_id=args.process_id,
            adapter_kind=args.adapter_kind,
            capabilities=args.capability,
            resources=_parse_resources(args),
            lease_seconds=args.lease_seconds,
        )
        return {
            "ok": True,
            "claim": claim.model_dump(mode="json") if claim else None,
        }

    if args.command == "control-run":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        if args.action == "pause":
            run = await service.pause_run(args.run_id, reason=args.reason)
        elif args.action == "resume":
            run = await service.resume_run(
                args.run_id,
                reason=args.reason,
                allow_contract_drift=args.allow_contract_drift,
            )
        elif args.action == "cancel":
            run = await service.cancel_run(args.run_id, reason=args.reason)
        else:
            raise ValueError(f"Unknown run action: {args.action}")
        return {"ok": True, "run": run.model_dump(mode="json")}

    if args.command == "work-once":
        pipeline = registry.get(args.pipeline)
        store = create_state_store(args.db)
        return await _work_once(
            store=store,
            registry=registry,
            pipeline=pipeline,
            run_id=args.run_id,
            worker_id=args.worker_id,
            process_id=args.process_id,
            adapter_kind=args.adapter_kind,
            capabilities=args.capability,
            resources=_parse_resources(args),
            lease_seconds=args.lease_seconds,
        )

    if args.command == "run-until-idle":
        if args.max_steps < 1:
            raise ValueError("--max-steps must be greater than zero")
        pipeline = registry.get(args.pipeline)
        store = create_state_store(args.db)
        steps: list[dict[str, Any]] = []
        idle = False
        limit_reached = False
        for _ in range(args.max_steps):
            result = await _work_once(
                store=store,
                registry=registry,
                pipeline=pipeline,
                run_id=args.run_id,
                worker_id=args.worker_id,
                process_id=args.process_id,
                adapter_kind=args.adapter_kind,
                capabilities=args.capability,
                resources=_parse_resources(args),
                lease_seconds=args.lease_seconds,
            )
            if not result["completed"]:
                idle = True
                break
            steps.append(result)
        else:
            limit_reached = True

        return {
            "ok": True,
            "idle": idle,
            "limit_reached": limit_reached,
            "completed_count": len(steps),
            "steps": [
                {
                    "document_id": item["claim"]["document_id"],
                    "process_id": item["claim"]["process"]["id"],
                    "capability": item["claim"]["process"].get("capability"),
                    "attempt": item["claim"]["attempt"],
                    "output_keys": sorted(item["output"]["values"]),
                    "refreshed_projection_ids": [
                        projection["id"]
                        for projection in item["refreshed_projections"]
                    ],
                }
                for item in steps
            ],
            "state": await _runtime_state(
                store=store,
                run_id=args.run_id,
                pipeline=pipeline,
                registry=registry,
                include_events=args.include_events,
            ),
        }

    if args.command == "complete-process":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        if args.output_file is not None and (
            args.value or args.metadata or args.artifact
        ):
            raise ValueError(
                "--output-file cannot be combined with --value, --metadata, or --artifact"
            )
        process_output = (
            _validate_json_contract(ProcessOutput, args.output_file)
            if args.output_file is not None
            else ProcessOutput(
                values=_parse_values(args.value),
                artifacts=_parse_artifacts(args.artifact),
                metadata=_parse_values(args.metadata),
            )
        )
        output, refreshed, schedule, spawned = await service.complete_process_output(
            run_id=args.run_id,
            document_id=args.document_id,
            process_id=args.process_id,
            output=process_output,
            pipeline_id=args.pipeline,
            worker_id=args.worker_id,
        )
        return {
            "ok": True,
            "output": output.model_dump(mode="json"),
            "refreshed_projection_ids": [
                projection.id for projection in refreshed
            ],
            "schedule": schedule.model_dump(mode="json"),
            "spawned_documents": [
                item.model_dump(mode="json") for item in spawned
            ],
        }

    if args.command == "status":
        store = create_state_store(args.db)
        return {
            "ok": True,
            "state": await _runtime_state(
                store=store,
                run_id=args.run_id,
                registry=registry,
                include_events=args.include_events,
            ),
        }

    if args.command == "queue-metrics":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        return {
            "ok": True,
            "metrics": (await service.queue_metrics(args.run_id)).model_dump(mode="json"),
        }

    if args.command == "capability-demands":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        return {
            "ok": True,
            "demands": (
                await service.capability_demands(
                    args.run_id,
                    stale_after_seconds=args.stale_after_seconds,
                )
            ).model_dump(mode="json"),
        }

    if args.command == "metrics-prometheus":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        queue_metrics = await service.queue_metrics(
            args.run_id,
            stale_after_seconds=args.stale_after_seconds,
        )
        capability_demands = await service.capability_demands(
            args.run_id,
            stale_after_seconds=args.stale_after_seconds,
        )
        return {
            "ok": True,
            "metrics": render_prometheus_metrics(queue_metrics, capability_demands),
        }

    if args.command == "health":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        health = await service.run_health(
            args.run_id,
            stale_after_seconds=args.stale_after_seconds,
        )
        return {
            "ok": True,
            "health": health.model_dump(mode="json"),
        }

    if args.command == "worker-health":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        workers = await service.worker_health(
            args.run_id,
            stale_after_seconds=args.stale_after_seconds,
        )
        return {
            "ok": True,
            "run_id": args.run_id,
            "worker_count": len(workers),
            "healthy_count": sum(1 for worker in workers if worker.healthy),
            "workers": [worker.model_dump(mode="json") for worker in workers],
        }

    if args.command == "audit-log":
        if args.limit < 1:
            raise ValueError("--limit must be greater than zero")
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        audit = await service.operator_audit(
            run_id=args.run_id,
            limit=args.limit,
            descending=True,
        )
        return {
            "ok": True,
            "audit": audit.model_dump(mode="json"),
        }

    if args.command == "document-lineage":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        lineage = await service.document_lineage(args.run_id)
        return {
            "ok": True,
            "lineage": lineage.model_dump(mode="json"),
        }

    if args.command == "run-results":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        results = await service.run_results(
            args.run_id,
            pipeline_id=args.pipeline,
            process_id=args.process_id,
            document_id=args.document_id,
            document_type=args.document_type,
            operation_type=args.operation_type,
        )
        if args.jsonl:
            for item in results.results:
                print(json.dumps(item.model_dump(mode="json"), sort_keys=True))
            return None
        return {
            "ok": True,
            "results": results.model_dump(mode="json"),
        }

    if args.command == "output-documents":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        page = await service.output_documents(
            args.run_id,
            pipeline_id=args.pipeline,
            process_id=args.process_id,
            document_id=args.document_id,
            source_document_type=args.source_document_type,
            output_document_id=args.output_document_id,
            document_type=args.document_type,
            relation=args.relation,
            media_type=args.media_type,
            limit=args.limit,
            offset=args.offset,
        )
        if args.jsonl:
            for item in page.output_documents:
                print(json.dumps(item.model_dump(mode="json"), sort_keys=True))
            return None
        return {
            "ok": True,
            "output_documents": page.model_dump(mode="json"),
        }

    if args.command == "run-reductions":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        reductions = await service.run_reductions(
            args.run_id,
            pipeline_id=args.pipeline,
            reduce_id=args.reduce_id,
        )
        return {
            "ok": True,
            "reductions": reductions.model_dump(mode="json"),
        }

    if args.command == "artifact-gc":
        service = RuntimeService(
            registry=registry,
            store=create_state_store(args.db),
            artifact_store_root=args.artifact_store_root,
        )
        plan = await service.artifact_gc(dry_run=not args.delete)
        return {"ok": True, "artifact_gc": plan.model_dump(mode="json")}

    if args.command == "run-retention":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        plan = await service.run_retention(
            before=_retention_cutoff(
                before=args.before,
                older_than_days=args.older_than_days,
            ),
            statuses=[RunStatus(status) for status in args.status] or None,
            dry_run=not args.delete,
        )
        return {"ok": True, "retention": plan.model_dump(mode="json")}

    if args.command == "stream-append":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        chunk = await service.append_stream_chunk(
            run_id=args.run_id,
            document_id=args.document_id,
            process_id=args.process_id,
            stream_id=args.stream_id,
            sequence=args.sequence,
            kind=args.kind,
            values=_parse_values(args.value),
            artifacts=_parse_artifacts(args.artifact),
            metadata=_parse_values(args.metadata),
        )
        return {
            "ok": True,
            "chunk": chunk.model_dump(mode="json"),
        }

    if args.command == "stream-list":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        chunks = await service.list_stream_chunks(
            run_id=args.run_id,
            document_id=args.document_id,
            process_id=args.process_id,
            stream_id=args.stream_id,
            after_sequence=args.after_sequence,
            limit=args.limit,
        )
        return {
            "ok": True,
            "run_id": args.run_id,
            "document_id": args.document_id,
            "process_id": args.process_id,
            "stream_id": args.stream_id,
            "chunk_count": len(chunks),
            "chunks": [chunk.model_dump(mode="json") for chunk in chunks],
        }

    if args.command == "stream-checkpoint":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        checkpoint = await service.put_stream_checkpoint(
            run_id=args.run_id,
            document_id=args.document_id,
            process_id=args.process_id,
            stream_id=args.stream_id,
            consumer_id=args.consumer_id,
            sequence=args.sequence,
            chunk_id=args.chunk_id,
            metadata=_parse_values(args.metadata),
        )
        return {
            "ok": True,
            "checkpoint": checkpoint.model_dump(mode="json"),
        }

    if args.command == "stream-checkpoint-get":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        checkpoint = await service.get_stream_checkpoint(
            run_id=args.run_id,
            document_id=args.document_id,
            process_id=args.process_id,
            stream_id=args.stream_id,
            consumer_id=args.consumer_id,
        )
        return {
            "ok": True,
            "checkpoint": (
                checkpoint.model_dump(mode="json") if checkpoint is not None else None
            ),
        }

    if args.command == "trace":
        service = RuntimeService(registry=registry, store=create_state_store(args.db))
        return {
            "ok": True,
            "trace": (
                await service.process_trace(
                    args.run_id,
                    document_id=args.document_id,
                    process_id=args.process_id,
                    operation_type=args.operation_type,
                )
            ).model_dump(mode="json"),
        }

    raise ValueError(f"Unknown command: {args.command}")


def _require_process_runtime_client() -> Any:
    try:
        from fala.client import ProcessRuntimeClient
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError(
            "HTTP control-plane commands require optional client dependencies; "
            "install `fala[client]` or `fala[web]`."
        ) from exc
    return ProcessRuntimeClient


def _add_carrier_runtime_db_run_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--db", required=True, help="Carrier runtime SQLite DB path or sqlite:// URL.")
    parser.add_argument("--run-id", required=True)


async def _carrier_runtime_command(args: argparse.Namespace) -> dict[str, Any] | None:
    backend = SQLiteRuntimeBackend(_carrier_runtime_db_path(args.db))
    if args.command == "carriers":
        if args.carrier_command == "list":
            carriers = await backend.list_carriers(
                run_id=args.run_id,
                carrier_type=args.carrier_type,
                limit=args.limit,
            )
            return _carrier_runtime_list_result("carriers", carriers, jsonl=args.jsonl)
        carrier = await backend.get_carrier(
            run_id=args.run_id,
            carrier_id=args.carrier_id,
        )
        return {
            "ok": carrier is not None,
            "carrier": carrier.model_dump(mode="json") if carrier is not None else None,
        }
    if args.command == "carrier-types":
        if args.carrier_type_command == "list":
            carrier_types = await backend.list_carrier_types(run_id=args.run_id)
            return _carrier_runtime_list_result(
                "carrier_types",
                carrier_types,
                jsonl=args.jsonl,
            )
        carrier_type = await backend.get_carrier_type(
            run_id=args.run_id,
            carrier_type_id=args.carrier_type_id,
        )
        return {
            "ok": carrier_type is not None,
            "carrier_type": carrier_type.model_dump(mode="json")
            if carrier_type is not None
            else None,
        }
    if args.command == "carrier-relations":
        if args.carrier_relation_command == "list":
            relations = await backend.list_carrier_relations(
                run_id=args.run_id,
                carrier_id=args.carrier_id,
                relation_type=args.relation_type,
            )
            return _carrier_runtime_list_result(
                "carrier_relations",
                relations,
                jsonl=args.jsonl,
            )
        relation = await backend.get_carrier_relation(
            run_id=args.run_id,
            relation_id=args.relation_id,
        )
        return {
            "ok": relation is not None,
            "carrier_relation": relation.model_dump(mode="json")
            if relation is not None
            else None,
        }
    if args.command == "observations":
        observations = await backend.list_observations(
            run_id=args.run_id,
            carrier_id=args.carrier_id,
        )
        return _carrier_runtime_list_result(
            "observations",
            observations,
            jsonl=args.jsonl,
        )
    if args.command == "events":
        events = await backend.list_events(
            run_id=args.run_id,
            carrier_id=args.carrier_id,
            after_sequence=args.after_sequence,
            limit=args.limit,
        )
        return _carrier_runtime_list_result("events", events, jsonl=args.jsonl)
    if args.command == "gates":
        gates = await backend.list_gates(
            run_id=args.run_id,
            carrier_id=args.carrier_id,
            status=CarrierGateStatus(args.status) if args.status else None,
        )
        return _carrier_runtime_list_result("gates", gates, jsonl=args.jsonl)
    if args.command == "projections":
        projections = await backend.list_projections(run_id=args.run_id)
        return _carrier_runtime_list_result(
            "projections",
            projections,
            jsonl=args.jsonl,
        )
    raise ValueError(f"Unknown Carrier runtime command: {args.command}")


def _carrier_runtime_list_result(
    key: str,
    items: list[Any],
    *,
    jsonl: bool,
) -> dict[str, Any] | None:
    payload = [item.model_dump(mode="json") for item in items]
    if jsonl:
        for item in payload:
            print(json.dumps(item, sort_keys=True))
        return None
    return {
        "ok": True,
        "count": len(payload),
        key: payload,
    }


def _carrier_runtime_db_path(target: str) -> str:
    parsed = urlparse(target)
    if parsed.scheme in {"sqlite", "sqlite3"}:
        if parsed.netloc and parsed.netloc != "localhost":
            raise ValueError("SQLite URL host must be empty or localhost")
        if parsed.netloc == "localhost":
            path = parsed.path
        elif target.startswith(f"{parsed.scheme}:////"):
            path = parsed.path
        else:
            path = parsed.path.lstrip("/")
        if not path:
            raise ValueError("SQLite URL must include a database path")
        return unquote(path)
    if parsed.scheme:
        raise ValueError(f"Unsupported Carrier runtime DB URL scheme: {parsed.scheme!r}")
    return target


async def _run_queue_work_from_args(
    args: argparse.Namespace,
    *,
    adapters: AdapterRegistry,
    renew_client: ProcessRuntimeClient | None,
) -> dict[str, Any] | None:
    broker_target = _queue_broker_target_from_args(args)
    if broker_target is not None:
        if args.max_claims < 1:
            raise ValueError("--max-claims must be greater than zero")
        transport = create_queue_broker_transport(broker_target)
        results: list[QueueResultEnvelope] = []
        work_ids: list[str] = []
        for _ in range(args.max_claims):
            work = await transport.claim_work(
                queue=args.queue,
                worker_id=args.worker_id,
                lease_seconds=args.lease_seconds,
                max_deliveries=args.max_deliveries,
            )
            if work is None:
                break
            work = assign_queue_work_worker(work, args.worker_id)
            try:
                result = await run_queue_work(
                    work,
                    adapters=adapters,
                    error_kind=args.error_kind,
                    renew_client=renew_client,
                    renew_interval_seconds=args.renew_interval_seconds,
                    lease_seconds=args.lease_seconds,
                )
                await transport.publish_result(result)
                await transport.complete_work(work.id)
            except BaseException as exc:
                await transport.release_work(work.id, error=str(exc))
                raise
            results.append(result)
            work_ids.append(work.id)
        return {
            "ok": True,
            "queue_broker": broker_target,
            "queue_db": args.queue_db,
            "processed_count": len(results),
            "work_ids": work_ids,
            "result_ids": [result.id for result in results],
            "renew_claim": bool(renew_client),
            "stats": await transport.stats(),
        }

    work_items = read_work_jsonl(_queue_text_source(args.work_file))
    work_items = [
        assign_queue_work_worker(work, args.worker_id)
        for work in work_items
    ]
    results = [
        await run_queue_work(
            work,
            adapters=adapters,
            error_kind=args.error_kind,
            renew_client=renew_client,
            renew_interval_seconds=args.renew_interval_seconds,
            lease_seconds=args.lease_seconds,
        )
        for work in work_items
    ]
    _write_queue_jsonl(results, args.result_file)
    return None


async def _work_once(
    *,
    store: StateStore,
    registry: PipelineRegistry,
    pipeline: PipelineSpec,
    run_id: str,
    worker_id: str,
    process_id: str | None = None,
    adapter_kind: str | None = None,
    capabilities: list[str] | None = None,
    resources: ResourceSpec | dict[str, Any] | None = None,
    lease_seconds: float = 300.0,
    adapters: AdapterRegistry | None = None,
) -> dict[str, Any]:
    scheduler = PipelineScheduler(pipeline, store)
    service = RuntimeService(registry=registry, store=store)
    claim = await service.claim_next(
        run_id=run_id,
        pipeline_id=pipeline.id,
        worker_id=worker_id,
        process_id=process_id,
        adapter_kind=adapter_kind,
        capabilities=capabilities,
        resources=resources,
        lease_seconds=lease_seconds,
    )
    if claim is None:
        return {"ok": True, "claim": None, "completed": False}

    step = _step_by_id(pipeline, claim.process.id)
    try:
        await store.append_event(
            ProcessEvent(
                run_id=claim.run_id,
                document_id=claim.document_id,
                process_id=claim.process.id,
                type="process.started",
                status=ProcessStatus.running,
                data={"worker_id": worker_id, "attempt": claim.attempt},
            )
        )
        output = await (adapters or AdapterRegistry.default()).run(
            step,
            claim.context,
            event_sink=store.append_event,
        )
    except Exception as exc:
        failure = await scheduler.record_process_failure(
            run_id=claim.run_id,
            document_id=claim.document_id,
            process_id=claim.process.id,
            error_kind="worker_error",
            data={
                "worker_id": worker_id,
                "error": str(exc),
                "error_kind": "worker_error",
            },
        )
        await service.sync_run_lifecycle(run_id)
        return {
            "ok": True,
            "completed": False,
            "claim": claim.model_dump(mode="json"),
            "error": str(exc),
            "failure": failure.model_dump(mode="json"),
            "schedule": failure.schedule.model_dump(mode="json"),
        }

    output, refreshed, schedule, spawned = await service.complete_process_output(
        run_id=claim.run_id,
        document_id=claim.document_id,
        process_id=claim.process.id,
        output=output,
        pipeline_id=pipeline.id,
        worker_id=worker_id,
    )
    return {
        "ok": True,
        "completed": True,
        "claim": claim.model_dump(mode="json"),
        "output": output.model_dump(mode="json"),
        "refreshed_projections": [
            projection.model_dump(mode="json")
            for projection in refreshed
        ],
        "schedule": schedule.model_dump(mode="json"),
        "spawned_documents": [
            item.model_dump(mode="json") for item in spawned
        ],
    }


async def _runtime_state(
    *,
    store: StateStore,
    run_id: str,
    pipeline: PipelineSpec | None = None,
    registry: PipelineRegistry | None = None,
    include_events: bool = False,
) -> dict[str, Any]:
    documents: list[dict[str, Any]] = []
    for document_id in await store.list_documents(run_id=run_id):
        pipeline_id = await store.get_document_pipeline_id(
            run_id=run_id,
            document_id=document_id,
        )
        statuses = await store.list_statuses(
            run_id=run_id,
            document_id=document_id,
        )
        claims = await store.list_claims(
            run_id=run_id,
            document_id=document_id,
        )
        outputs = await store.list_outputs(
            run_id=run_id,
            document_id=document_id,
        )
        projections = await store.list_projections(
            run_id=run_id,
            document_id=document_id,
        )
        stream_chunks = await store.list_stream_chunks(
            run_id=run_id,
            document_id=document_id,
        )
        stream_checkpoints = await store.list_stream_checkpoints(
            run_id=run_id,
            document_id=document_id,
        )
        events = (
            await store.list_events(run_id=run_id, document_id=document_id)
            if include_events
            else []
        )
        event_count = await store.count_events(run_id=run_id, document_id=document_id)
        document_pipeline = pipeline if pipeline and pipeline.id == pipeline_id else None
        if document_pipeline is None and registry is not None and pipeline_id:
            try:
                document_pipeline = registry.get(pipeline_id)
            except Exception:
                document_pipeline = None
        documents.append(
            build_runtime_document_state(
                document_id=document_id,
                pipeline_id=pipeline_id,
                pipeline=document_pipeline,
                statuses=statuses,
                claims=claims,
                outputs=outputs,
                projections=projections,
                stream_chunks=stream_chunks,
                stream_checkpoints=stream_checkpoints,
                events=events,
                event_count=event_count,
            )
        )
    return build_runtime_state(run_id=run_id, documents=documents).model_dump(mode="json")


def _pipeline_dir(args: argparse.Namespace) -> Path:
    if args.pipeline_dir:
        return Path(args.pipeline_dir)
    cwd = Path.cwd()
    candidates = [
        cwd / "control-plane" / "examples" / "pipelines",
        cwd / "examples" / "pipelines",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def _default_deployment_base_url(format: str, container_port: int) -> str:
    if format == "docker-compose":
        return f"http://fala-control-plane:{container_port}"
    if format == "kubernetes":
        return f"http://fala-control-plane:{container_port}"
    raise ValueError(f"Unsupported deployment format: {format}")


def _add_resource_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--cpu-cores", type=float, default=None)
    parser.add_argument("--memory-mb", type=int, default=None)
    parser.add_argument("--disk-mb", type=int, default=None)
    parser.add_argument("--gpu-count", type=int, default=None)
    parser.add_argument(
        "--resource-label",
        action="append",
        default=[],
        help="Resource label provided by this worker. Repeatable.",
    )
    parser.add_argument(
        "--resource-unit",
        action="append",
        default=[],
        help="Named resource capacity as KEY=VALUE. Repeatable.",
    )


def _parse_resources(args: argparse.Namespace) -> ResourceSpec:
    return ResourceSpec(
        cpu_cores=getattr(args, "cpu_cores", None),
        memory_mb=getattr(args, "memory_mb", None),
        disk_mb=getattr(args, "disk_mb", None),
        gpu_count=getattr(args, "gpu_count", None),
        labels=list(getattr(args, "resource_label", [])),
        units=_parse_resource_units(getattr(args, "resource_unit", [])),
    )


def _parse_resource_units(values: list[str]) -> dict[str, float]:
    units: dict[str, float] = {}
    for value in values:
        if "=" not in value:
            raise ValueError(f"--resource-unit must be KEY=VALUE, got {value!r}")
        key, item = value.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"--resource-unit key cannot be empty: {value!r}")
        try:
            units[key] = float(item)
        except ValueError as exc:
            raise ValueError(
                f"--resource-unit value must be numeric for {key!r}: {item!r}"
            ) from exc
    return units


class _CollectingQueueTransport:
    def __init__(self) -> None:
        self.work: list[QueueWorkEnvelope] = []
        self.results: list[QueueResultEnvelope] = []

    async def publish_work(self, envelope: QueueWorkEnvelope) -> None:
        self.work.append(envelope)

    async def publish_result(self, envelope: QueueResultEnvelope) -> None:
        self.results.append(envelope)


def _queue_broker_target_from_args(args: argparse.Namespace) -> str | None:
    return getattr(args, "queue_broker", None) or getattr(args, "queue_db", None)


def _require_queue_broker_target(args: argparse.Namespace) -> str:
    target = _queue_broker_target_from_args(args)
    if target is None:
        raise ValueError(
            "queue broker target is required; pass --queue-broker or --queue-db"
        )
    return target


def _queue_text_source(source: str) -> str | Path | io.StringIO:
    if source == "-":
        return io.StringIO(sys.stdin.read())
    return Path(source)


def _write_queue_jsonl(items: Iterable[BaseModel], target: str) -> None:
    if target == "-":
        write_jsonl(items, sys.stdout)
        return
    write_jsonl(items, Path(target))


def _write_json_output(target: str, payload: dict[str, Any]) -> None:
    path = Path(target)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _parse_command(values: list[str] | None) -> list[str]:
    if not values:
        return []
    if values and values[0] == "--":
        values = values[1:]
    return list(values)


def _parse_env(values: list[str]) -> dict[str, str]:
    env: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise ValueError(f"--env must be KEY=VALUE, got {value!r}")
        key, item = value.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"--env key cannot be empty: {value!r}")
        env[key] = item
    return env


def _retention_cutoff(
    *,
    before: str | None,
    older_than_days: float | None,
) -> datetime:
    if before is not None:
        try:
            parsed = datetime.fromisoformat(before.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("--before must be an ISO datetime") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    if older_than_days is None:
        raise ValueError("--before or --older-than-days is required")
    return datetime.now(timezone.utc) - timedelta(days=older_than_days)


def _parse_run_config(args: argparse.Namespace) -> dict[str, Any]:
    resource_pools = _parse_run_resource_pools(
        getattr(args, "resource_pool", []),
    )
    return {"resource_pools": resource_pools} if resource_pools else {}


def _parse_run_resource_pools(values: list[str]) -> dict[str, dict[str, Any]]:
    pools: dict[str, dict[str, Any]] = {}
    for value in values:
        if "=" not in value:
            raise ValueError(f"--resource-pool must be POOL.KEY=VALUE, got {value!r}")
        target, raw_value = value.split("=", 1)
        if "." not in target:
            raise ValueError(f"--resource-pool must include pool and key: {value!r}")
        pool_id, key = target.split(".", 1)
        pool_id = pool_id.strip()
        key = key.strip()
        if not pool_id or not key:
            raise ValueError(f"--resource-pool must include pool and key: {value!r}")
        pool = pools.setdefault(pool_id, {})
        if key == "cpu_cores":
            pool[key] = float(raw_value)
        elif key in {"memory_mb", "disk_mb", "gpu_count"}:
            pool[key] = int(raw_value)
        elif key.startswith("units."):
            unit_id = key.removeprefix("units.").strip()
            if not unit_id:
                raise ValueError(f"--resource-pool unit key cannot be empty: {value!r}")
            pool.setdefault("units", {})[unit_id] = float(raw_value)
        else:
            raise ValueError(f"Unknown --resource-pool key {key!r}")
    return pools


def _pipeline_summary(
    pipeline,
    *,
    registry: PipelineRegistry | None = None,
) -> dict[str, Any]:
    summary = {
        "id": pipeline.id,
        "title": pipeline.title,
        "description": pipeline.description,
        "tags": pipeline.tags,
        "version": pipeline.version,
        "input_values": pipeline.input_values,
        "steps": [
            {
                "id": step.id,
                "title": step.title,
                "description": step.description,
                "tags": step.tags,
                "capability": step.capability,
                "adapter_kind": step.adapter.kind,
                "needs": step.needs,
                "priority": step.priority,
                "max_concurrency": step.max_concurrency,
                "resource_pool": step.resource_pool,
                "resources": step.resources.model_dump(mode="json"),
                "sla": step.sla.model_dump(mode="json"),
            }
            for step in pipeline.steps
        ],
        "combines": [combine.id for combine in pipeline.combines],
        "reduces": [reduce.id for reduce in pipeline.reduces],
    }
    if registry is not None:
        package_id = registry.pipeline_package_id(pipeline.id)
        source = registry.pipeline_source(pipeline.id)
        if package_id is not None:
            summary["package_id"] = package_id
        if source is not None:
            summary["source"] = source
    return summary


def _package_summary(package: WorkflowPackageSpec) -> dict[str, Any]:
    return {
        "id": package.id,
        "title": package.title,
        "description": package.description,
        "tags": package.tags,
        "version": package.version,
        "document_types": [
            document_type.model_dump(mode="json")
            for document_type in package.document_types
        ],
        "document_relations": [
            relation.model_dump(mode="json")
            for relation in package.document_relations
        ],
        "operation_types": [
            operation.model_dump(mode="json")
            for operation in package.operation_types
        ],
        "artifact_kinds": [
            artifact_kind.model_dump(mode="json")
            for artifact_kind in package.artifact_kinds
        ],
        "capabilities": [
            capability.model_dump(mode="json")
            for capability in package.capabilities
        ],
        "secrets": [
            secret.model_dump(mode="json")
            for secret in package.secrets
        ],
        "pipelines": package.pipelines,
        "workers": [
            {
                "id": worker.id,
                "title": worker.title,
                "description": worker.description,
                "tags": worker.tags,
                "capabilities": worker.capabilities,
                "pipeline_id": worker.pipeline_id,
                "process_id": worker.process_id,
                "adapter_kind": worker.adapter_kind,
                "command": worker.command,
                "cwd": worker.cwd,
                "env": worker.env,
                "timeout_seconds": worker.timeout_seconds,
                "resources": worker.resources.model_dump(mode="json"),
                "secrets": list(worker.secrets),
                "sandbox": worker.sandbox.model_dump(mode="json"),
            }
            for worker in package.workers
        ],
    }


def _worker_command_summary(
    *,
    package_id: str,
    worker: WorkflowWorkerSpec,
    pipeline_dir: Path,
    base_url: str,
    run_id: str,
) -> dict[str, Any]:
    argv = [
        "process-runtime-worker",
        "--pipeline-dir",
        str(pipeline_dir),
        "--base-url",
        base_url,
        "--run-id",
        run_id,
        "--package-id",
        package_id,
        "--package-worker",
        worker.id,
    ]
    return {
        "package_id": package_id,
        "worker_id": worker.id,
        "pipeline_id": worker.pipeline_id,
        "process_id": worker.process_id,
        "capabilities": worker.capabilities,
        "resources": worker.resources.model_dump(mode="json"),
        "secrets": list(worker.secrets),
        "sandbox": worker.sandbox.model_dump(mode="json"),
        "adapter_kind": worker.adapter_kind,
        "argv": argv,
        "shell": " ".join(shlex.quote(part) for part in argv),
    }


def _validate_subprocess_commands(pipelines: list[PipelineSpec]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for pipeline in pipelines:
        for step in pipeline.steps:
            if step.adapter.kind != "subprocess":
                continue
            command = step.adapter.command or []
            reason = _subprocess_command_issue(command, cwd=step.adapter.cwd)
            if reason is None:
                continue
            issues.append(
                {
                    "pipeline_id": pipeline.id,
                    "process_id": step.id,
                    "command": command,
                    "reason": reason,
                }
            )
    return issues


def _validate_package_worker_commands(registry: PipelineRegistry) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for package in registry.packages():
        package_source = registry.package_source(package.id)
        package_root = (
            Path(package_source).parent
            if package_source is not None
            else None
        )
        for worker in package.workers:
            command = worker.command
            reason = _subprocess_command_issue(
                command,
                cwd=worker.cwd or (str(package_root) if package_root is not None else None),
            )
            if reason is None:
                continue
            issues.append(
                {
                    "package_id": package.id,
                    "worker_id": worker.id,
                    "pipeline_id": worker.pipeline_id,
                    "process_id": worker.process_id,
                    "command": command,
                    "reason": reason,
                }
            )
    return issues


def _subprocess_command_issue(command: list[str], *, cwd: str | None = None) -> str | None:
    executable = str(command[0]) if command else ""
    if not executable:
        return "missing executable"

    has_path_separator = "/" in executable or "\\" in executable
    if not has_path_separator:
        if shutil.which(executable):
            return _command_file_argument_issue(command, cwd=cwd)
        return f"executable {executable!r} not found on PATH"

    issue = _existing_executable_path_issue(executable, cwd=cwd)
    if issue is not None:
        return issue
    return _command_file_argument_issue(command, cwd=cwd)


def _existing_executable_path_issue(executable: str, *, cwd: str | None = None) -> str | None:
    path = _resolve_command_path(executable, cwd=cwd)
    if not path.exists():
        return f"executable path does not exist: {path}"
    if not path.is_file():
        return f"executable path is not a file: {path}"
    if not os.access(path, os.X_OK):
        return f"executable path is not executable: {path}"
    return None


def _command_file_argument_issue(command: list[str], *, cwd: str | None = None) -> str | None:
    if not command:
        return None
    executable_name = Path(command[0]).name.lower()
    if executable_name not in _SCRIPT_LAUNCHERS:
        return None
    for arg in command[1:]:
        if not _looks_like_script_path(arg):
            continue
        path = _resolve_command_path(arg, cwd=cwd)
        if not path.exists():
            return f"command file path does not exist: {path}"
        if not path.is_file():
            return f"command file path is not a file: {path}"
    return None


_SCRIPT_LAUNCHERS = {
    "bash",
    "node",
    "python",
    "python3",
    "ruby",
    "sh",
}


def _looks_like_script_path(value: str) -> bool:
    if not value or value.startswith("-"):
        return False
    if value in {"run", "uv", "python", "python3"}:
        return False
    path = Path(value)
    return "/" in value or "\\" in value or bool(path.suffix)


def _resolve_command_path(value: str, *, cwd: str | None = None) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        base = Path(cwd).expanduser() if cwd else Path.cwd()
        path = base / path
    return path.resolve()


def _validate_json_contract(model: type[BaseModel], source: str) -> BaseModel:
    raw = _read_json_source(source)
    try:
        return model.model_validate(raw)
    except Exception as exc:
        raise ValueError(f"{model.__name__} validation failed: {exc}") from exc


def _read_json_source(source: str) -> Any:
    text = sys.stdin.read() if source == "-" else Path(source).read_text(encoding="utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {source!r}: {exc}") from exc


def _read_yaml_object(source: str, *, label: str) -> dict[str, Any]:
    data = _read_yaml_value(source, label=label)
    if not isinstance(data, dict):
        raise ValueError(f"{label} must contain an object: {source}")
    return data


def _read_yaml_value(source: str, *, label: str) -> Any:
    text = sys.stdin.read() if source == "-" else Path(source).read_text(encoding="utf-8")
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid {label} {source!r}: {exc}") from exc
    return data


def _copy_scaffold_tuple_map(
    value: dict[str, tuple[str, ...]] | None,
) -> dict[str, list[str]] | None:
    if value is None:
        return None
    return {key: list(items) for key, items in value.items()}


def _copy_scaffold_schema_map(
    value: dict[str, dict[str, Any]] | None,
) -> dict[str, dict[str, Any]] | None:
    if value is None:
        return None
    return {
        key: json.loads(json.dumps(schema))
        for key, schema in value.items()
    }


def _copy_scaffold_stream_map(
    value: dict[str, tuple[StreamSpec, ...]] | None,
) -> dict[str, list[StreamSpec]] | None:
    if value is None:
        return None
    return {
        key: [
            StreamSpec.model_validate(stream.model_dump(mode="json", by_alias=True))
            for stream in streams
        ]
        for key, streams in value.items()
    }


def _normalize_scaffold_extensions(values: Iterable[str]) -> list[str]:
    extensions: list[str] = []
    for raw_value in values:
        value = raw_value.strip().lower()
        if not value:
            continue
        if not value.startswith("."):
            value = f".{value}"
        if value not in extensions:
            extensions.append(value)
    return extensions


def _parse_scaffold_extension_map(values: Iterable[str]) -> dict[str, list[str]] | None:
    mapping: dict[str, list[str]] = {}
    for raw_value in values:
        if "=" not in raw_value:
            raise ValueError(
                f"Invalid scaffold artifact extension {raw_value!r}: expected STEP=EXT"
            )
        step_id, extension = raw_value.split("=", 1)
        step_id = step_id.strip()
        if not step_id:
            raise ValueError(
                f"Invalid scaffold artifact extension {raw_value!r}: missing step id"
            )
        ProcessSpec(id=step_id, adapter={"kind": "queue", "queue": "validate.id"})
        normalized = _normalize_scaffold_extensions([extension])
        if not normalized:
            raise ValueError(
                f"Invalid scaffold artifact extension {raw_value!r}: missing extension"
            )
        existing = mapping.setdefault(step_id, [])
        for item in normalized:
            if item not in existing:
                existing.append(item)
    return mapping or None


def _merge_scaffold_extension_maps(
    base: dict[str, list[str]] | None,
    extra: dict[str, list[str]] | None,
) -> dict[str, list[str]] | None:
    if base is None and extra is None:
        return None
    merged: dict[str, list[str]] = {
        step_id: list(extensions)
        for step_id, extensions in (base or {}).items()
    }
    for step_id, extensions in (extra or {}).items():
        existing = merged.setdefault(step_id, [])
        for extension in extensions:
            if extension not in existing:
                existing.append(extension)
    return merged


def _parse_scaffold_schema_file_map(
    values: Iterable[str],
    *,
    label: str,
) -> dict[str, dict[str, Any]] | None:
    mapping: dict[str, dict[str, Any]] = {}
    for raw_value in values:
        step_id, source = _parse_scaffold_step_value(raw_value, label=label)
        mapping[step_id] = _read_yaml_object(source, label=f"{label} schema")
    return mapping or None


def _merge_scaffold_schema_maps(
    base: dict[str, dict[str, Any]] | None,
    extra: dict[str, dict[str, Any]] | None,
) -> dict[str, dict[str, Any]] | None:
    if base is None and extra is None:
        return None
    merged: dict[str, dict[str, Any]] = {
        step_id: json.loads(json.dumps(schema))
        for step_id, schema in (base or {}).items()
    }
    for step_id, schema in (extra or {}).items():
        merged[step_id] = json.loads(json.dumps(schema))
    return merged


def _merge_scaffold_policy_maps(
    base: dict[str, dict[str, Any]] | None,
    extra: dict[str, dict[str, Any]] | None,
) -> dict[str, dict[str, Any]] | None:
    if base is None and extra is None:
        return None
    merged: dict[str, dict[str, Any]] = {
        step_id: json.loads(json.dumps(policy))
        for step_id, policy in (base or {}).items()
    }
    for step_id, policy in (extra or {}).items():
        existing = merged.get(step_id, {})
        merged[step_id] = _deep_merge_scaffold_policy(
            existing,
            json.loads(json.dumps(policy)),
        )
    return merged


def _deep_merge_scaffold_policy(
    base: dict[str, Any],
    extra: dict[str, Any],
) -> dict[str, Any]:
    merged = json.loads(json.dumps(base))
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_scaffold_policy(merged[key], value)
        else:
            merged[key] = value
    return merged


def _parse_scaffold_stream_file_map(
    values: Iterable[str],
) -> dict[str, list[StreamSpec]] | None:
    mapping: dict[str, list[StreamSpec]] = {}
    for raw_value in values:
        step_id, source = _parse_scaffold_step_value(
            raw_value,
            label="stream contract",
        )
        raw_streams = _read_yaml_value(source, label="stream contract")
        mapping[step_id] = _coerce_scaffold_stream_specs(raw_streams, source=source)
    return mapping or None


def _coerce_scaffold_stream_specs(value: Any, *, source: str) -> list[StreamSpec]:
    if isinstance(value, dict) and "streams" in value:
        value = value["streams"]
    if isinstance(value, dict):
        value = [value]
    if not isinstance(value, list):
        raise ValueError(f"stream contract must contain object or list: {source}")
    streams: list[StreamSpec] = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError(f"stream contract entries must be objects: {source}")
        streams.append(StreamSpec.model_validate(item))
    return streams


def _merge_scaffold_stream_maps(
    base: dict[str, list[StreamSpec]] | None,
    extra: dict[str, list[StreamSpec]] | None,
) -> dict[str, list[StreamSpec]] | None:
    if base is None and extra is None:
        return None
    merged: dict[str, list[StreamSpec]] = {}
    for step_id, streams in (base or {}).items():
        merged[step_id] = [
            StreamSpec.model_validate(stream.model_dump(mode="json", by_alias=True))
            for stream in streams
        ]
    for step_id, streams in (extra or {}).items():
        merged[step_id] = [
            StreamSpec.model_validate(stream.model_dump(mode="json", by_alias=True))
            for stream in streams
        ]
    return merged


_SCAFFOLD_STEP_POLICY_FIELDS = {
    "adapter",
    "config",
    "description",
    "max_concurrency",
    "priority",
    "resource_pool",
    "resources",
    "retry",
    "sla",
    "tags",
    "timeout_seconds",
    "title",
    "wait_for_children",
    "when",
}


def _parse_scaffold_step_policy_map(
    values: Iterable[str],
) -> dict[str, dict[str, Any]] | None:
    mapping: dict[str, dict[str, Any]] = {}
    for raw_value in values:
        step_id, source = _parse_scaffold_step_value(
            raw_value,
            label="step policy",
        )
        policy = _read_yaml_object(source, label="step policy")
        invalid = sorted(set(policy) - _SCAFFOLD_STEP_POLICY_FIELDS)
        if invalid:
            raise ValueError(
                f"Invalid scaffold step policy {source!r}: unsupported "
                f"{', '.join(invalid)}"
            )
        mapping[step_id] = policy
    return mapping or None


def _parse_scaffold_step_value(value: str, *, label: str) -> tuple[str, str]:
    if "=" not in value:
        raise ValueError(f"Invalid scaffold {label} {value!r}: expected STEP=PATH")
    step_id, source = value.split("=", 1)
    step_id = step_id.strip()
    source = source.strip()
    if not step_id:
        raise ValueError(f"Invalid scaffold {label} {value!r}: missing step id")
    if not source:
        raise ValueError(f"Invalid scaffold {label} {value!r}: missing path")
    ProcessSpec(id=step_id, adapter={"kind": "queue", "queue": "validate.id"})
    return step_id, source


def _scaffold_input_from_args(args: argparse.Namespace) -> dict[str, Any]:
    document_value_schema = (
        _read_yaml_object(args.document_value_schema, label="document value schema")
        if args.document_value_schema
        else None
    )
    document_metadata_schema = (
        _read_yaml_object(args.document_metadata_schema, label="document metadata schema")
        if args.document_metadata_schema
        else None
    )
    if args.blueprint or args.blueprint_file:
        if args.blueprint_file:
            blueprint = _scaffold_blueprint_from_file(args.blueprint_file)
        else:
            blueprint = get_scaffold_blueprint(args.blueprint)
            assert blueprint is not None
        artifact_extensions_by_step = _merge_scaffold_extension_maps(
            _copy_scaffold_tuple_map(blueprint.artifact_extensions_by_step),
            _parse_scaffold_extension_map(args.artifact_extension),
        )
        artifact_value_schema_by_step = _merge_scaffold_schema_maps(
            _copy_scaffold_schema_map(blueprint.artifact_value_schema_by_step),
            _parse_scaffold_schema_file_map(
                args.artifact_value_schema,
                label="artifact value schema",
            ),
        )
        capability_output_schema_by_step = _merge_scaffold_schema_maps(
            _copy_scaffold_schema_map(blueprint.capability_output_schema_by_step),
            _parse_scaffold_schema_file_map(
                args.capability_output_schema,
                label="capability output schema",
            ),
        )
        capability_streams_by_step = _merge_scaffold_stream_maps(
            _copy_scaffold_stream_map(blueprint.capability_streams_by_step),
            _parse_scaffold_stream_file_map(args.stream_contract),
        )
        step_policy_by_step = _merge_scaffold_policy_maps(
            _copy_scaffold_schema_map(blueprint.step_policy_by_step),
            _parse_scaffold_step_policy_map(args.step_policy),
        )
        return {
            "blueprint_id": blueprint.id,
            "steps": list(blueprint.steps),
            "document_type": args.document_type or blueprint.document_type,
            "document_media_types": list(
                args.document_media_type or blueprint.document_media_types
            ),
            "document_extensions": _normalize_scaffold_extensions(
                args.document_extension or blueprint.document_extensions
            ),
            "document_value_schema": (
                json.loads(
                    json.dumps(
                        document_value_schema
                        if document_value_schema is not None
                        else blueprint.document_value_schema
                    )
                )
                if (
                    document_value_schema is not None
                    or blueprint.document_value_schema is not None
                )
                else None
            ),
            "document_metadata_schema": (
                json.loads(
                    json.dumps(
                        document_metadata_schema
                        if document_metadata_schema is not None
                        else blueprint.document_metadata_schema
                    )
                )
                if (
                    document_metadata_schema is not None
                    or blueprint.document_metadata_schema is not None
                )
                else None
            ),
            "additional_document_types": [
                DocumentTypeSpec.model_validate(
                    document_type.model_dump(mode="json")
                )
                for document_type in blueprint.additional_document_types
            ],
            "additional_document_relations": [
                DocumentRelationSpec.model_validate(
                    relation.model_dump(mode="json")
                )
                for relation in blueprint.additional_document_relations
            ],
            "operation_types": [
                OperationTypeSpec.model_validate(
                    operation.model_dump(mode="json")
                )
                for operation in blueprint.operation_types
            ],
            "operation_type_by_step": dict(blueprint.operation_type_by_step or {}),
            "needs_by_step": _copy_scaffold_tuple_map(blueprint.needs_by_step),
            "artifact_kind_by_step": dict(blueprint.artifact_kind_by_step),
            "capability_by_step": dict(blueprint.capability_by_step),
            "accepted_document_types_by_step": _copy_scaffold_tuple_map(
                blueprint.accepted_document_types_by_step
            ),
            "emitted_document_types_by_step": _copy_scaffold_tuple_map(
                blueprint.emitted_document_types_by_step
            ),
            "artifact_media_types_by_step": _copy_scaffold_tuple_map(
                blueprint.artifact_media_types_by_step
            ),
            "artifact_extensions_by_step": artifact_extensions_by_step,
            "artifact_value_schema_by_step": artifact_value_schema_by_step,
            "capability_output_schema_by_step": capability_output_schema_by_step,
            "capability_streams_by_step": capability_streams_by_step,
            "step_policy_by_step": step_policy_by_step,
            "step_guidance_by_step": _copy_scaffold_schema_map(
                blueprint.step_guidance_by_step
            ),
        }

    return {
        "blueprint_id": None,
        "steps": _parse_scaffold_steps(args.steps),
        "document_type": args.document_type or "generic_document",
        "document_media_types": args.document_media_type,
        "document_extensions": _normalize_scaffold_extensions(args.document_extension),
        "document_value_schema": document_value_schema or document_source_value_schema(),
        "document_metadata_schema": document_metadata_schema,
        "additional_document_types": [],
        "additional_document_relations": [],
        "operation_types": [],
        "operation_type_by_step": None,
        "needs_by_step": None,
        "artifact_kind_by_step": None,
        "capability_by_step": None,
        "accepted_document_types_by_step": None,
        "emitted_document_types_by_step": None,
        "artifact_media_types_by_step": None,
        "artifact_extensions_by_step": _parse_scaffold_extension_map(args.artifact_extension),
        "artifact_value_schema_by_step": _parse_scaffold_schema_file_map(
            args.artifact_value_schema,
            label="artifact value schema",
        ),
        "capability_output_schema_by_step": _parse_scaffold_schema_file_map(
            args.capability_output_schema,
            label="capability output schema",
        ),
        "capability_streams_by_step": _parse_scaffold_stream_file_map(
            args.stream_contract
        ),
        "step_policy_by_step": _parse_scaffold_step_policy_map(args.step_policy),
        "step_guidance_by_step": None,
    }


def _scaffold_blueprint_from_file(path: str) -> Any:
    return scaffold_blueprint_from_mapping(
        _read_yaml_object(path, label="scaffold blueprint"),
        source=str(path),
    )


def _init_project_blueprints(
    args: argparse.Namespace,
) -> tuple[list[ScaffoldBlueprint], dict[str, str]]:
    blueprints: list[ScaffoldBlueprint] = []
    blueprint_sources: dict[str, str] = {}

    for blueprint_id in args.blueprint or []:
        blueprint = get_scaffold_blueprint(blueprint_id)
        if blueprint is None:
            raise ValueError(f"Unknown scaffold blueprint: {blueprint_id}")
        blueprints.append(blueprint)

    for path in args.blueprint_file or []:
        blueprint = _scaffold_blueprint_from_file(path)
        blueprints.append(blueprint)
        blueprint_sources[blueprint.id] = str(Path(path).expanduser().resolve())

    if not blueprints:
        blueprints = list(SCAFFOLD_BLUEPRINTS.values())

    blueprint_ids = [blueprint.id for blueprint in blueprints]
    duplicate_ids = sorted(
        blueprint_id
        for blueprint_id, count in Counter(blueprint_ids).items()
        if count > 1
    )
    if duplicate_ids:
        raise ValueError(
            "init-project blueprints must not contain duplicates: "
            + ", ".join(duplicate_ids)
        )
    return blueprints, blueprint_sources


def _init_project_command(args: argparse.Namespace) -> dict[str, Any]:
    blueprints, blueprint_sources = _init_project_blueprints(args)
    blueprint_ids = [blueprint.id for blueprint in blueprints]
    output_dir = Path(args.output_dir)
    pipeline_dir = output_dir / "pipelines"
    output_dir.mkdir(parents=True, exist_ok=True)
    pipeline_dir.mkdir(parents=True, exist_ok=True)

    packages: list[dict[str, Any]] = []
    package_dirs: list[Path] = []
    for blueprint in blueprints:
        package_dir = pipeline_dir / blueprint.id
        package_dirs.append(package_dir)
        packages.append(
            _scaffold_workflow_package(
                output_dir=package_dir,
                package_id=blueprint.id,
                pipeline_id=f"{blueprint.id}_flow",
                steps=list(blueprint.steps),
                adapter_kind=args.adapter_kind,
                title=blueprint.title,
                document_type=blueprint.document_type,
                document_media_types=list(blueprint.document_media_types),
                document_extensions=list(blueprint.document_extensions),
                document_value_schema=(
                    json.loads(json.dumps(blueprint.document_value_schema))
                    if blueprint.document_value_schema is not None
                    else None
                ),
                document_metadata_schema=(
                    json.loads(json.dumps(blueprint.document_metadata_schema))
                    if blueprint.document_metadata_schema is not None
                    else None
                ),
                additional_document_types=[
                    DocumentTypeSpec.model_validate(
                        document_type.model_dump(mode="json")
                    )
                    for document_type in blueprint.additional_document_types
                ],
                additional_document_relations=[
                    DocumentRelationSpec.model_validate(
                        relation.model_dump(mode="json")
                    )
                    for relation in blueprint.additional_document_relations
                ],
                operation_types=[
                    OperationTypeSpec.model_validate(
                        operation.model_dump(mode="json")
                    )
                    for operation in blueprint.operation_types
                ],
                operation_type_by_step=dict(blueprint.operation_type_by_step or {}),
                needs_by_step=_copy_scaffold_tuple_map(blueprint.needs_by_step),
                artifact_kind_by_step=dict(blueprint.artifact_kind_by_step),
                capability_by_step=dict(blueprint.capability_by_step),
                accepted_document_types_by_step=_copy_scaffold_tuple_map(
                    blueprint.accepted_document_types_by_step
                ),
                emitted_document_types_by_step=_copy_scaffold_tuple_map(
                    blueprint.emitted_document_types_by_step
                ),
                artifact_media_types_by_step=_copy_scaffold_tuple_map(
                    blueprint.artifact_media_types_by_step
                ),
                artifact_extensions_by_step=_copy_scaffold_tuple_map(
                    blueprint.artifact_extensions_by_step
                ),
                artifact_value_schema_by_step=_copy_scaffold_schema_map(
                    blueprint.artifact_value_schema_by_step
                ),
                capability_output_schema_by_step=_copy_scaffold_schema_map(
                    blueprint.capability_output_schema_by_step
                ),
                capability_streams_by_step=_copy_scaffold_stream_map(
                    blueprint.capability_streams_by_step
                ),
                step_policy_by_step=_copy_scaffold_schema_map(
                    blueprint.step_policy_by_step
                ),
                step_guidance_by_step=_copy_scaffold_schema_map(
                    blueprint.step_guidance_by_step
                ),
                blueprint_id=blueprint.id,
            )
        )

    root_created = [
        _write_new_file(
            output_dir / "fala-project.yaml",
            _init_project_yaml(
                project_id=args.project_id,
                blueprints=blueprints,
                blueprint_sources=blueprint_sources,
                adapter_kind=args.adapter_kind,
            ),
        ),
        _write_new_file(
            output_dir / "README.md",
            _init_project_readme(
                project_id=args.project_id,
                blueprints=blueprints,
                adapter_kind=args.adapter_kind,
            ),
        ),
        _write_new_file(
            output_dir / "Makefile",
            _init_project_makefile(package_dirs=package_dirs),
        ),
        _write_new_file(
            output_dir / "source-list.example.csv",
            _init_project_source_list_csv(blueprints=blueprints),
        ),
        _write_new_file(
            output_dir / "document-routes.example.yaml",
            _init_project_routes_yaml(blueprints=blueprints),
        ),
    ]
    root_created.extend(
        _init_project_relation_sample_files(
            pipeline_dir=pipeline_dir,
            blueprints=blueprints,
        )
    )
    return {
        "ok": True,
        "project_id": args.project_id,
        "output_dir": str(output_dir),
        "pipeline_dir": str(pipeline_dir),
        "adapter_kind": args.adapter_kind,
        "blueprints": blueprint_ids,
        "package_count": len(packages),
        "packages": packages,
        "created": [
            str(path)
            for path in [
                *root_created,
                *(
                    Path(path)
                    for package in packages
                    for path in package["created"]
                ),
            ]
        ],
    }


def _project_doctor_command(args: argparse.Namespace) -> dict[str, Any]:
    project_yaml = (
        Path(args.project_yaml).expanduser()
        if args.project_yaml
        else Path(args.project_dir).expanduser() / "fala-project.yaml"
    ).resolve()
    report = build_project_readiness_report(project_yaml)
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return {
            "ok": report["ok"],
            "output": str(output),
            "error_count": report["error_count"],
            "warning_count": report["warning_count"],
        }
    return report


def _project_spec_command(args: argparse.Namespace) -> dict[str, Any]:
    project_yaml = (
        Path(args.project_yaml).expanduser()
        if args.project_yaml
        else Path(args.project_dir).expanduser() / "fala-project.yaml"
    ).resolve()
    registry = PipelineRegistry.from_directory(project_pipeline_dir(project_yaml))
    spec = build_project_spec_report(
        project_yaml,
        registry=registry,
        base_url=args.base_url,
        run_id=args.run_id,
    )
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(spec, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return {
            "ok": spec["ok"],
            "output": str(output),
            "project_id": spec.get("project_id"),
            "package_count": spec["package_index"]["package_count"],
            "worker_count": spec["worker_commands"]["worker_count"],
        }
    return spec


def _project_check_command(args: argparse.Namespace) -> dict[str, Any]:
    project_yaml, pipeline_dir = _project_report_paths(args)
    registry = PipelineRegistry.from_directory(pipeline_dir)
    db = (
        runtime_db_diagnostics(args.db, ensure_schema=args.ensure_schema)
        if args.db
        else None
    )
    bundle = verify_project_bundle(args.bundle) if args.bundle else None
    report = build_project_bootstrap_check(
        project_yaml,
        registry=registry,
        base_url=args.base_url,
        run_id=args.run_id,
        db=db,
        bundle=bundle,
    )
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return {
            "ok": report["ok"],
            "output": str(output),
            "project_id": report["project_id"],
            "check_count": report["check_count"],
            "failed_check_count": report["failed_check_count"],
        }
    return report


async def _project_smoke_command(args: argparse.Namespace) -> dict[str, Any]:
    if args.max_steps < 1:
        raise ValueError("--max-steps must be greater than zero")

    project_yaml, pipeline_dir = _project_report_paths(args)
    registry = PipelineRegistry.from_directory(pipeline_dir)
    service = RuntimeService(
        registry=registry,
        store=create_state_store(args.db),
    )
    run_input, route_report = build_project_runtime_run_input(
        project_yaml,
        registry=registry,
        run_id=args.run_id,
        title=args.title,
        existing_run_policy=args.existing_run,
        existing_document_policy=args.existing_document,
        metadata={
            **_parse_values(args.metadata),
            "project_smoke": True,
        },
    )
    run, schedules = await service.create_run_with_documents(
        run_input,
        route_report=route_report,
    )

    executable_adapter_kinds = (
        set(args.adapter_kind)
        if args.adapter_kind
        else {"subprocess", "http", "queue"}
    )
    queue_adapter = _ProjectSmokeQueueAdapter(registry)
    adapters = AdapterRegistry.default()
    adapters.register("queue", queue_adapter)
    pipeline_ids = _project_smoke_pipeline_ids(run_input, schedules)
    worker_routes = queue_adapter.routes_by_pipeline(pipeline_ids)

    steps: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    idle = False
    limit_reached = False
    local_resources = _parse_resources(args)

    for _ in range(args.max_steps):
        result = await _project_smoke_work_once(
            store=service.store,
            registry=registry,
            adapters=adapters,
            run_id=run.id,
            pipeline_ids=pipeline_ids,
            worker_routes=worker_routes if "queue" in executable_adapter_kinds else [],
            worker_id=args.worker_id,
            executable_adapter_kinds=executable_adapter_kinds,
            local_resources=local_resources,
        )
        if result is None:
            idle = True
            break
        if result.get("completed"):
            steps.append(_project_smoke_step_summary(result))
            continue
        failures.append(_project_smoke_failure_summary(result))
        break
    else:
        limit_reached = True

    run = await service.sync_run_lifecycle(run.id)
    state = await service.load_state_model(run.id, include_events=args.include_state)
    health = await service.run_health(run.id)
    results = await service.run_results(run.id)
    reductions = await service.run_reductions(run.id)
    report = {
        "ok": (
            run.status == RunStatus.completed
            and not failures
            and not limit_reached
            and health.status != "critical"
        ),
        "project_yaml": str(project_yaml),
        "pipeline_dir": str(pipeline_dir),
        "db": args.db,
        "run_id": run.id,
        "run_status": run.status.value,
        "run_outcome": run.outcome.value if run.outcome else None,
        "document_count": len(schedules),
        "route_report": route_report,
        "pipeline_ids": pipeline_ids,
        "executable_adapter_kinds": sorted(executable_adapter_kinds),
        "queue_worker_count": len(worker_routes),
        "completed_count": len(steps),
        "failed_count": len(failures),
        "idle": idle,
        "limit_reached": limit_reached,
        "max_steps": args.max_steps,
        "steps": steps,
        "failures": failures,
        "state_summary": state.summary.model_dump(mode="json"),
        "health": health.model_dump(mode="json"),
        "result_count": len(results.results),
        "reduction_count": len(reductions.reductions),
    }
    if args.include_state:
        report["state"] = state.model_dump(mode="json")
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return {
            "ok": report["ok"],
            "output": str(output),
            "run_id": run.id,
            "run_status": run.status.value,
            "completed_count": len(steps),
            "failed_count": len(failures),
            "health_status": health.status,
        }
    return report


async def _project_smoke_work_once(
    *,
    store: StateStore,
    registry: PipelineRegistry,
    adapters: AdapterRegistry,
    run_id: str,
    pipeline_ids: list[str],
    worker_routes: list[dict[str, Any]],
    worker_id: str,
    executable_adapter_kinds: set[str],
    local_resources: ResourceSpec,
) -> dict[str, Any] | None:
    for pipeline_id in pipeline_ids:
        pipeline = registry.get(pipeline_id)
        for adapter_kind in ("subprocess", "http"):
            if adapter_kind not in executable_adapter_kinds:
                continue
            result = await _work_once(
                store=store,
                registry=registry,
                pipeline=pipeline,
                run_id=run_id,
                worker_id=f"{worker_id}-{adapter_kind}",
                adapter_kind=adapter_kind,
                resources=local_resources,
                adapters=adapters,
            )
            if result.get("claim") is not None:
                return result

    for route in worker_routes:
        pipeline = registry.get(route["worker"].pipeline_id)
        result = await _work_once(
            store=store,
            registry=registry,
            pipeline=pipeline,
            run_id=run_id,
            worker_id=f"{worker_id}-{route['worker'].id}",
            process_id=route["worker"].process_id,
            adapter_kind="queue",
            capabilities=route["worker"].capabilities,
            resources=route["worker"].resources,
            adapters=adapters,
        )
        if result.get("claim") is not None:
            return result
    return None


def _project_smoke_pipeline_ids(
    run_input: RuntimeRunInput,
    schedules: list[ScheduleResult],
) -> list[str]:
    pipeline_ids = {
        item.pipeline_id or run_input.pipeline_id
        for item in run_input.documents
        if item.pipeline_id or run_input.pipeline_id
    }
    pipeline_ids.update(schedule.pipeline_id for schedule in schedules)
    return sorted(str(item) for item in pipeline_ids if item)


class _ProjectSmokeQueueAdapter:
    def __init__(self, registry: PipelineRegistry) -> None:
        self._routes: list[dict[str, Any]] = []
        for package in registry.packages():
            package_source = registry.package_source(package.id)
            package_root = (
                Path(package_source).parent.resolve()
                if package_source is not None
                else None
            )
            for worker in package.workers:
                self._routes.append(
                    {
                        "package_id": package.id,
                        "package_root": package_root,
                        "worker": worker,
                    }
                )

    def routes_by_pipeline(self, pipeline_ids: list[str]) -> list[dict[str, Any]]:
        selected = [
            route
            for route in self._routes
            if route["worker"].pipeline_id in set(pipeline_ids)
        ]
        selected.sort(key=lambda route: (route["worker"].pipeline_id, route["worker"].id))
        return selected

    async def run(
        self,
        spec: ProcessSpec,
        context: ProcessExecutionContext,
        *,
        event_sink: Any | None = None,
    ) -> ProcessOutput:
        route = self._route_for(spec, context)
        worker: WorkflowWorkerSpec = route["worker"]
        adapter = ExternalCommandAdapter(
            command=worker.command,
            cwd=_project_smoke_worker_cwd(route["package_root"], worker),
            env=worker.env,
            timeout_seconds=worker.timeout_seconds,
        )
        output = await adapter.run(spec, context, event_sink=event_sink)
        runtime_metadata = dict(output.metadata.get("process_runtime") or {})
        runtime_metadata.update(
            {
                "project_smoke_worker_id": worker.id,
                "project_smoke_package_id": route["package_id"],
            }
        )
        metadata = dict(output.metadata)
        metadata["process_runtime"] = runtime_metadata
        return output.model_copy(update={"metadata": metadata})

    def _route_for(
        self,
        spec: ProcessSpec,
        context: ProcessExecutionContext,
    ) -> dict[str, Any]:
        candidates = [
            route
            for route in self._routes
            if _project_smoke_worker_matches(route["worker"], spec, context)
        ]
        if not candidates:
            raise ProcessAdapterError(
                f"No declared package worker for queue process {context.pipeline_id}/{spec.id}"
            )
        candidates.sort(
            key=lambda route: (
                route["worker"].process_id != spec.id,
                route["package_id"],
                route["worker"].id,
            )
        )
        return candidates[0]


def _project_smoke_worker_matches(
    worker: WorkflowWorkerSpec,
    spec: ProcessSpec,
    context: ProcessExecutionContext,
) -> bool:
    if worker.pipeline_id != context.pipeline_id:
        return False
    if worker.process_id is not None and worker.process_id != spec.id:
        return False
    if worker.capabilities and spec.capability not in set(worker.capabilities):
        return False
    return True


def _project_smoke_worker_cwd(
    package_root: Path | None,
    worker: WorkflowWorkerSpec,
) -> str | None:
    if worker.cwd is None:
        return str(package_root) if package_root is not None else None
    path = Path(worker.cwd).expanduser()
    if not path.is_absolute() and package_root is not None:
        path = package_root / path
    return str(path)


def _project_smoke_step_summary(result: dict[str, Any]) -> dict[str, Any]:
    claim = result["claim"]
    output = result["output"]
    return {
        "document_id": claim["document_id"],
        "pipeline_id": claim["pipeline_id"],
        "process_id": claim["process"]["id"],
        "capability": claim["process"].get("capability"),
        "adapter_kind": claim["process"]["adapter"]["kind"],
        "worker_id": claim.get("worker_id"),
        "attempt": claim["attempt"],
        "artifact_count": len(output.get("artifacts") or []),
        "stream_chunk_count": len(output.get("stream_chunks") or []),
        "output_keys": sorted((output.get("values") or {}).keys()),
        "refreshed_projection_ids": [
            projection["id"]
            for projection in result.get("refreshed_projections") or []
        ],
        "spawned_document_count": len(result.get("spawned_documents") or []),
    }


def _project_smoke_failure_summary(result: dict[str, Any]) -> dict[str, Any]:
    claim = result.get("claim") or {}
    process = claim.get("process") or {}
    return {
        "document_id": claim.get("document_id"),
        "pipeline_id": claim.get("pipeline_id"),
        "process_id": process.get("id"),
        "capability": process.get("capability"),
        "adapter_kind": (process.get("adapter") or {}).get("kind"),
        "worker_id": claim.get("worker_id"),
        "attempt": claim.get("attempt"),
        "error": result.get("error"),
        "failure": result.get("failure"),
    }


def _project_secrets_command(args: argparse.Namespace) -> dict[str, Any]:
    project_yaml, pipeline_dir = _project_report_paths(args)
    registry = PipelineRegistry.from_directory(pipeline_dir)
    readiness = build_project_readiness_report(project_yaml, registry=registry)
    project_id = str(readiness.get("project_id") or "") or None
    inventory = build_project_secret_inventory(
        project_id=project_id,
        registry=registry,
    )
    env_template = render_project_env_template(
        inventory,
        include_auth_placeholders=not args.no_auth_placeholders,
    )
    payload = {
        "ok": True,
        "project_id": project_id,
        "project_yaml": str(project_yaml),
        "pipeline_dir": str(pipeline_dir),
        "secrets": inventory,
        "env_template": env_template,
    }
    if args.env_output:
        env_output = Path(args.env_output)
        env_output.parent.mkdir(parents=True, exist_ok=True)
        env_output.write_text(env_template, encoding="utf-8")
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return {
            "ok": True,
            "output": str(output),
            "env_output": str(Path(args.env_output)) if args.env_output else None,
            "project_id": project_id,
            "secret_count": inventory["secret_count"],
            "env_var_count": inventory["env_var_count"],
            "required_count": inventory["required_count"],
        }
    return payload


def _project_bundle_command(args: argparse.Namespace) -> dict[str, Any]:
    project_yaml, pipeline_dir = _project_report_paths(args)
    registry = PipelineRegistry.from_directory(pipeline_dir)
    return write_project_bundle(
        project_yaml,
        registry=registry,
        output=args.output,
        base_url=args.base_url,
        run_id=args.run_id,
        bundle_name=args.bundle_name,
    )


def _project_report_paths(args: argparse.Namespace) -> tuple[Path, Path]:
    project_yaml = (
        Path(args.project_yaml).expanduser()
        if args.project_yaml
        else Path(args.project_dir).expanduser() / "fala-project.yaml"
    ).resolve()
    pipeline_dir = (
        Path(args.pipeline_dir).expanduser().resolve()
        if args.pipeline_dir
        else project_pipeline_dir(project_yaml)
    )
    return project_yaml, pipeline_dir


def _project_runtime_service(
    args: argparse.Namespace,
    *,
    artifact_store_root: str | None = None,
) -> tuple[Path, Path, PipelineRegistry, RuntimeService]:
    project_yaml, pipeline_dir = _project_report_paths(args)
    registry = PipelineRegistry.from_directory(pipeline_dir)
    service = RuntimeService(
        registry=registry,
        store=create_state_store(args.db),
        artifact_store_root=artifact_store_root,
    )
    return project_yaml, pipeline_dir, registry, service


async def _project_history_context(
    *,
    args: argparse.Namespace,
    project_yaml: Path,
    registry: PipelineRegistry,
    service: RuntimeService,
) -> tuple[dict[str, Any], str | None, dict[str, Any]]:
    readiness = build_project_readiness_report(project_yaml, registry=registry)
    project_id = str(readiness.get("project_id") or "") or None
    runs = await service.list_run_summaries(limit=args.run_limit)
    history = build_project_run_history(
        project_id=project_id,
        registry=registry,
        runs=runs,
        package_id=args.package_id,
        pipeline_id=args.pipeline_id,
        document_type=args.document_type,
        limit=None,
    )
    return readiness, project_id, history


async def _project_supervision_report_from_args(
    *,
    args: argparse.Namespace,
    project_id: str | None,
    registry: PipelineRegistry,
    service: RuntimeService,
    history: dict[str, Any],
) -> dict[str, Any]:
    stuck_status = ProcessStatus(args.stuck_status) if args.stuck_status else None
    dead_letter_pages: list[dict[str, Any]] = []
    stuck_work_pages: list[dict[str, Any]] = []
    stream_lag_pages: list[dict[str, Any]] = []
    for run in history["runs"]:
        run_id = str(run["run_id"])
        dead_letter_pages.append(
            (
                await service.dead_letter_queue(
                    run_id,
                    pipeline_id=args.pipeline_id,
                    document_type=args.document_type,
                    operation_type=args.operation_type,
                    limit=1000,
                )
            ).model_dump(mode="json")
        )
        stuck_work_pages.append(
            (
                await service.stuck_work(
                    run_id,
                    status=stuck_status,
                    pipeline_id=args.pipeline_id,
                    document_type=args.document_type,
                    operation_type=args.operation_type,
                    waiting_after_seconds=args.waiting_after_seconds,
                    queued_after_seconds=args.queued_after_seconds,
                    running_after_seconds=args.running_after_seconds,
                    limit=1000,
                )
            ).model_dump(mode="json")
        )
        stream_lag_pages.append(
            (
                await service.stream_lag(
                    run_id,
                    pipeline_id=args.pipeline_id,
                    document_type=args.document_type,
                    operation_type=args.operation_type,
                    consumer_id=args.consumer_id,
                    min_lag=args.min_lag,
                    over_limit=args.over_limit,
                    limit=1000,
                )
            ).model_dump(mode="json")
        )
    supervision = build_project_supervision_report(
        project_id=project_id,
        registry=registry,
        runs=history["runs"],
        dead_letter_pages=dead_letter_pages,
        stuck_work_pages=stuck_work_pages,
        stream_lag_pages=stream_lag_pages,
        package_id=args.package_id,
        pipeline_id=args.pipeline_id,
        document_type=args.document_type,
        operation_type=args.operation_type,
        limit=args.limit,
    )
    supervision["filters"].update(
        {
            "stuck_status": stuck_status.value if stuck_status is not None else None,
            "waiting_after_seconds": args.waiting_after_seconds,
            "queued_after_seconds": args.queued_after_seconds,
            "running_after_seconds": args.running_after_seconds,
            "operation_type": args.operation_type,
            "consumer_id": args.consumer_id,
            "min_lag": args.min_lag,
            "over_limit": args.over_limit,
            "run_limit": args.run_limit,
        }
    )
    return supervision


async def _project_operations_report_from_args(
    *,
    args: argparse.Namespace,
    project_id: str | None,
    registry: PipelineRegistry,
    service: RuntimeService,
    history: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    supervision = await _project_supervision_report_from_args(
        args=args,
        project_id=project_id,
        registry=registry,
        service=service,
        history=history,
    )
    health_reports = [
        (
            await service.run_health(
                str(run["run_id"]),
                stale_after_seconds=args.stale_after_seconds,
            )
        ).model_dump(mode="json")
        for run in history["runs"]
    ]
    operations = build_project_operations_report(
        project_id=project_id,
        registry=registry,
        runs=history["runs"],
        health_reports=health_reports,
        supervision=supervision,
        package_id=args.package_id,
        pipeline_id=args.pipeline_id,
        document_type=args.document_type,
        operation_type=args.operation_type,
        limit=args.limit,
    )
    operations["filters"].update(
        {
            "stuck_status": args.stuck_status,
            "waiting_after_seconds": args.waiting_after_seconds,
            "queued_after_seconds": args.queued_after_seconds,
            "running_after_seconds": args.running_after_seconds,
            "operation_type": args.operation_type,
            "consumer_id": args.consumer_id,
            "min_lag": args.min_lag,
            "over_limit": args.over_limit,
            "stale_after_seconds": args.stale_after_seconds,
            "run_limit": args.run_limit,
        }
    )
    return operations, supervision


def _project_report_envelope(
    *,
    args: argparse.Namespace,
    report_key: str,
    project_yaml: Path,
    pipeline_dir: Path,
    project_id: str | None,
    report: dict[str, Any],
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "ok": True,
        "configured": True,
        "project_id": project_id,
        "project_yaml": str(project_yaml),
        "pipeline_dir": str(pipeline_dir),
        report_key: report,
    }
    payload.update(extra or {})
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return {
            "ok": True,
            "output": str(output),
            "project_id": project_id,
            "status": report.get("status"),
        }
    return payload


async def _project_supervision_command(args: argparse.Namespace) -> dict[str, Any]:
    project_yaml, pipeline_dir, registry, service = _project_runtime_service(args)
    _, project_id, history = await _project_history_context(
        args=args,
        project_yaml=project_yaml,
        registry=registry,
        service=service,
    )
    supervision = await _project_supervision_report_from_args(
        args=args,
        project_id=project_id,
        registry=registry,
        service=service,
        history=history,
    )
    return _project_report_envelope(
        args=args,
        report_key="supervision",
        project_yaml=project_yaml,
        pipeline_dir=pipeline_dir,
        project_id=project_id,
        report=supervision,
    )


async def _project_operations_command(args: argparse.Namespace) -> dict[str, Any]:
    project_yaml, pipeline_dir, registry, service = _project_runtime_service(args)
    _, project_id, history = await _project_history_context(
        args=args,
        project_yaml=project_yaml,
        registry=registry,
        service=service,
    )
    operations, supervision = await _project_operations_report_from_args(
        args=args,
        project_id=project_id,
        registry=registry,
        service=service,
        history=history,
    )
    return _project_report_envelope(
        args=args,
        report_key="operations",
        project_yaml=project_yaml,
        pipeline_dir=pipeline_dir,
        project_id=project_id,
        report=operations,
        extra={"supervision": supervision},
    )


async def _project_alerts_command(args: argparse.Namespace) -> dict[str, Any]:
    project_yaml, pipeline_dir, registry, service = _project_runtime_service(args)
    _, project_id, history = await _project_history_context(
        args=args,
        project_yaml=project_yaml,
        registry=registry,
        service=service,
    )
    operations, _ = await _project_operations_report_from_args(
        args=args,
        project_id=project_id,
        registry=registry,
        service=service,
        history=history,
    )
    alerts = build_project_alert_report(project_yaml, operations=operations)
    return _project_report_envelope(
        args=args,
        report_key="alerts",
        project_yaml=project_yaml,
        pipeline_dir=pipeline_dir,
        project_id=project_id,
        report=alerts,
        extra={"operations": operations},
    )


async def _project_lifecycle_command(args: argparse.Namespace) -> dict[str, Any]:
    project_yaml, pipeline_dir, registry, service = _project_runtime_service(
        args,
        artifact_store_root=args.artifact_store_root,
    )
    _, project_id, history = await _project_history_context(
        args=args,
        project_yaml=project_yaml,
        registry=registry,
        service=service,
    )
    artifact_gc = (
        (await service.artifact_gc(dry_run=True)).model_dump(mode="json")
        if not args.skip_artifact_gc
        else None
    )
    statuses = [RunStatus(status) for status in args.status] or None
    plan = build_project_lifecycle_report(
        project_yaml,
        runs=history["runs"],
        before=args.before,
        older_than_days=args.older_than_days,
        statuses=statuses,
        artifact_gc=artifact_gc,
        dry_run=not args.delete,
        limit=args.limit,
    )
    deleted_run_ids: set[str] = set()
    row_counts: Counter[str] = Counter()
    if args.delete:
        for item in plan["retention"]["runs"]:
            run_id = str(item["run_id"])
            row_counts.update(await service.store.delete_run(run_id))
            deleted_run_ids.add(run_id)
        artifact_gc = (
            (await service.artifact_gc(dry_run=True)).model_dump(mode="json")
            if not args.skip_artifact_gc
            else None
        )
        plan = build_project_lifecycle_report(
            project_yaml,
            runs=history["runs"],
            before=args.before,
            older_than_days=args.older_than_days,
            statuses=statuses,
            artifact_gc=artifact_gc,
            dry_run=False,
            deleted_run_ids=deleted_run_ids,
            row_counts=dict(row_counts),
            limit=args.limit,
        )
    plan["filters"].update(
        {
            "package_id": args.package_id,
            "pipeline_id": args.pipeline_id,
            "document_type": args.document_type,
            "include_artifact_gc": not args.skip_artifact_gc,
            "run_limit": args.run_limit,
        }
    )
    return _project_report_envelope(
        args=args,
        report_key="lifecycle",
        project_yaml=project_yaml,
        pipeline_dir=pipeline_dir,
        project_id=project_id,
        report=plan,
    )


def _init_project_yaml(
    *,
    project_id: str,
    blueprints: list[ScaffoldBlueprint],
    blueprint_sources: dict[str, str],
    adapter_kind: str,
) -> str:
    packages = []
    for blueprint in blueprints:
        package = {
            "id": blueprint.id,
            "blueprint": blueprint.id,
            "package_dir": f"pipelines/{blueprint.id}",
            "package_id": blueprint.id,
            "pipeline_id": f"{blueprint.id}_flow",
            "document_type": blueprint.document_type,
        }
        if blueprint.id in blueprint_sources:
            package["blueprint_source"] = blueprint_sources[blueprint.id]
        packages.append(package)
    return yaml.safe_dump(
        {
            "project": project_id,
            "adapter_kind": adapter_kind,
            "pipeline_dir": "pipelines",
            "source_list": "source-list.example.csv",
            "routes": "document-routes.example.yaml",
            "mixed_run_input": "run-input.mixed.json",
            "run_id": "run_mixed_sample",
            "outputs": {
                "package_index": "package-index.json",
                "worker_commands": "worker-commands.json",
                "deployment_compose": "deployment.docker-compose.json",
            },
            "alerts": {
                "enabled": True,
                "rules": [
                    {
                        "id": "project_status_critical",
                        "metric": "status",
                        "operator": "==",
                        "threshold": "critical",
                        "severity": "critical",
                        "message": "Project operations status is critical.",
                    },
                    {
                        "id": "worker_deficit_present",
                        "metric": "queue.worker_deficit_count",
                        "operator": ">",
                        "threshold": 0,
                        "severity": "critical",
                        "message": "Project needs more healthy workers for queued work.",
                    },
                    {
                        "id": "dead_letter_present",
                        "metric": "supervision.dead_letter_count",
                        "operator": ">",
                        "threshold": 0,
                        "severity": "critical",
                        "message": "Project has dead-lettered process instances.",
                    },
                    {
                        "id": "stream_lag_present",
                        "metric": "supervision.stream_lag_count",
                        "operator": ">",
                        "threshold": 0,
                        "severity": "warning",
                        "message": "Project has stream lag.",
                    },
                ],
            },
            "lifecycle": {
                "run_retention": {
                    "enabled": True,
                    "older_than_days": 30,
                    "statuses": ["completed", "failed", "cancelled"],
                },
                "artifact_gc": {
                    "enabled": True,
                },
            },
            "packages": packages,
        },
        sort_keys=False,
        allow_unicode=False,
    )


def _init_project_readme(
    *,
    project_id: str,
    blueprints: list[ScaffoldBlueprint],
    adapter_kind: str,
) -> str:
    package_lines = "\n".join(
        f"- `pipelines/{blueprint.id}`: {blueprint.title}"
        for blueprint in blueprints
    )
    first_blueprint_id = blueprints[0].id
    return (
        f"# {_title_from_id(project_id)} Fala workspace\n"
        "\n"
        "Generated by `fala init-project`.\n"
        "\n"
        "This workspace contains multiple independent Fala workflow packages under "
        "`pipelines/`. Each package owns its document type, artifact kinds, "
        "capabilities, pipeline YAML, contracts, sample input, source-list sample, "
        "step programs, and package Makefile.\n"
        "\n"
        f"Adapter mode: `{adapter_kind}`.\n"
        "\n"
        "## Packages\n"
        "\n"
        f"{package_lines}\n"
        "\n"
        "## Bootstrap\n"
        "\n"
        "```bash\n"
        "make bootstrap\n"
        "make serve\n"
        "```\n"
        "\n"
        "Useful root targets:\n"
        "\n"
        "- `make validate`: validate every package under `pipelines/`\n"
        "- `make doctor`: run package readiness checks\n"
        "- `make project-doctor`: run root workspace readiness from `fala-project.yaml`\n"
        "- `make project-check`: run aggregate bootstrap checks\n"
        "- `make project-smoke`: create mixed sample run and execute local worker commands\n"
        "- `make db-doctor`: check runtime database connectivity and schema\n"
        "- `make project-spec`: export one bootstrap spec/runbook for the workspace\n"
        "- `make project-secrets`: export secret inventory and `.env.example`\n"
        "- `make project-bundle`: export a portable workspace archive\n"
        "- `make project-bundle-verify`: verify archive paths and checksums\n"
        "- `make project-supervision`: export dead-letter/stuck-work/stream-lag report\n"
        "- `make project-operations`: export health/backlog/worker-demand report\n"
        "- `make project-alerts`: evaluate project alert policy over operations\n"
        "- `make project-lifecycle`: plan project retention and artifact GC\n"
        "- `make mixed-source-list`: compile one mixed source-list into an auto-routed run input\n"
        "- `make create-mixed`: create one run from the mixed auto-routed input\n"
        "- `make package-index`: write package release digests to `package-index.json`\n"
        "- `make worker-commands`: write package worker commands to `worker-commands.json`\n"
        "- `make deployment-compose`: write a Docker Compose deployment envelope to `deployment.docker-compose.json`\n"
        "- `make package-bootstrap`: run each package Makefile bootstrap target\n"
        "- `make serve`: start the Fala API/web panel for this workspace\n"
        "\n"
        "Root `document-routes.example.yaml` is an editable routing policy. "
        "Keep it close to intake rules when real source lists or folders contain "
        "mixed document types.\n"
        "Root `fala-project.yaml` also contains editable alert rules over "
        "project operations metrics and lifecycle retention policy.\n"
        "\n"
        "Work inside one package when iterating on a specific document workflow:\n"
        "\n"
        "```bash\n"
        f"make -C pipelines/{first_blueprint_id} bootstrap\n"
        f"make -C pipelines/{first_blueprint_id} create\n"
        f"make -C pipelines/{first_blueprint_id} run-local\n"
        "```\n"
    )


def _init_project_makefile(*, package_dirs: list[Path]) -> str:
    package_dir_value = " ".join(str(path.relative_to(path.parents[1])) for path in package_dirs)
    lines = [
        "FALA ?= fala",
        "DB ?= runtime.db",
        "PIPELINE_DIR ?= pipelines",
        "SOURCE_LIST ?= source-list.example.csv",
        "ROUTES ?= document-routes.example.yaml",
        "MIXED_RUN_INPUT ?= run-input.mixed.json",
        "PROJECT_DOCTOR ?= project-doctor.json",
        "PROJECT_CHECK ?= project-check.json",
        "PROJECT_SMOKE ?= project-smoke.json",
        "DB_DOCTOR ?= db-doctor.json",
        "PROJECT_SPEC ?= project-spec.json",
        "PROJECT_SECRETS ?= project-secrets.json",
        "ENV_EXAMPLE ?= .env.example",
        "PROJECT_BUNDLE ?= fala-project-bundle.tar.gz",
        "PROJECT_SUPERVISION ?= project-supervision.json",
        "PROJECT_OPERATIONS ?= project-operations.json",
        "PROJECT_ALERTS ?= project-alerts.json",
        "PROJECT_LIFECYCLE ?= project-lifecycle.json",
        "PACKAGE_INDEX ?= package-index.json",
        "WORKER_COMMANDS ?= worker-commands.json",
        "DEPLOYMENT_COMPOSE ?= deployment.docker-compose.json",
        "RUN_ID ?= run_mixed_sample",
        "SMOKE_RUN_ID ?= run_smoke_sample",
        "BASE_URL ?= http://localhost:8000",
        "IMAGE ?= fala:latest",
        "WORKER_IMAGE ?= fala-worker:latest",
        "CONTAINER_PIPELINE_DIR ?= /app/pipelines",
        f"PACKAGE_DIRS := {package_dir_value}",
        "",
        ".PHONY: bootstrap validate doctor db-doctor project-doctor project-check project-smoke project-spec project-secrets project-bundle project-bundle-verify project-supervision project-operations project-alerts project-lifecycle mixed-source-list create-mixed package-index worker-commands deployment-compose package-bootstrap serve clean",
        "",
        "bootstrap: validate doctor project-doctor db-doctor project-spec project-secrets project-check mixed-source-list package-index package-bootstrap",
        "",
        "validate:",
        "\t$(FALA) --pipeline-dir $(PIPELINE_DIR) validate --json --check-commands",
        "",
        "doctor:",
        "\t$(FALA) --pipeline-dir $(PIPELINE_DIR) package-doctor",
        "",
        "project-doctor:",
        "\t$(FALA) project-doctor --project-dir . --output $(PROJECT_DOCTOR)",
        "",
        "project-check:",
        "\t$(FALA) project-check --project-dir . --db $(DB) --ensure-schema --base-url $(BASE_URL) --run-id $(RUN_ID) --output $(PROJECT_CHECK)",
        "",
        "project-smoke:",
        "\t$(FALA) project-smoke --project-dir . --db $(DB) --run-id $(SMOKE_RUN_ID) --output $(PROJECT_SMOKE)",
        "",
        "db-doctor:",
        "\t$(FALA) db-doctor --db $(DB) --ensure-schema --output $(DB_DOCTOR)",
        "",
        "project-spec:",
        "\t$(FALA) project-spec --project-dir . --base-url $(BASE_URL) --run-id $(RUN_ID) --output $(PROJECT_SPEC)",
        "",
        "project-secrets:",
        "\t$(FALA) project-secrets --project-dir . --output $(PROJECT_SECRETS) --env-output $(ENV_EXAMPLE)",
        "",
        "project-bundle:",
        "\t$(FALA) project-bundle --project-dir . --base-url $(BASE_URL) --run-id $(RUN_ID) --output $(PROJECT_BUNDLE)",
        "",
        "project-bundle-verify:",
        "\t$(FALA) project-bundle-verify $(PROJECT_BUNDLE)",
        "",
        "project-supervision:",
        "\t$(FALA) project-supervision --project-dir . --db $(DB) --output $(PROJECT_SUPERVISION)",
        "",
        "project-operations:",
        "\t$(FALA) project-operations --project-dir . --db $(DB) --output $(PROJECT_OPERATIONS)",
        "",
        "project-alerts:",
        "\t$(FALA) project-alerts --project-dir . --db $(DB) --output $(PROJECT_ALERTS)",
        "",
        "project-lifecycle:",
        "\t$(FALA) project-lifecycle --project-dir . --db $(DB) --output $(PROJECT_LIFECYCLE)",
        "",
        "mixed-source-list:",
        "\t$(FALA) --pipeline-dir $(PIPELINE_DIR) discover-documents \\",
        "\t  --source-list $(SOURCE_LIST) \\",
        "\t  --route $(ROUTES) \\",
        "\t  --auto-route \\",
        "\t  --run-id $(RUN_ID) \\",
        "\t  > $(MIXED_RUN_INPUT)",
        "",
        "create-mixed:",
        "\t$(FALA) create-project-run \\",
        "\t  --project-dir . \\",
        "\t  --db $(DB) \\",
        "\t  --run-id $(RUN_ID)",
        "",
        "package-index:",
        "\t$(FALA) --pipeline-dir $(PIPELINE_DIR) package-index \\",
        "\t  --output $(PACKAGE_INDEX)",
        "",
        "worker-commands:",
        "\t$(FALA) --pipeline-dir $(PIPELINE_DIR) worker-commands \\",
        "\t  --base-url $(BASE_URL) \\",
        "\t  --run-id $(RUN_ID) \\",
        "\t  > $(WORKER_COMMANDS)",
        "",
        "deployment-compose:",
        "\t$(FALA) --pipeline-dir $(PIPELINE_DIR) deployment \\",
        "\t  --format docker-compose \\",
        "\t  --run-id $(RUN_ID) \\",
        "\t  --image $(IMAGE) \\",
        "\t  --worker-image $(WORKER_IMAGE) \\",
        "\t  --container-pipeline-dir $(CONTAINER_PIPELINE_DIR) \\",
        "\t  > $(DEPLOYMENT_COMPOSE)",
        "",
        "package-bootstrap:",
        "\t@for dir in $(PACKAGE_DIRS); do $(MAKE) -C $$dir bootstrap; done",
        "",
        "serve:",
        "\t$(FALA) --pipeline-dir $(PIPELINE_DIR) serve --db $(DB)",
        "",
        "clean:",
        "\t@for dir in $(PACKAGE_DIRS); do $(MAKE) -C $$dir clean; done",
        "\trm -f $(DB) $(MIXED_RUN_INPUT) $(PROJECT_DOCTOR) $(PROJECT_CHECK) $(PROJECT_SMOKE) $(DB_DOCTOR) $(PROJECT_SPEC) $(PROJECT_SECRETS) $(ENV_EXAMPLE) $(PROJECT_BUNDLE) $(PROJECT_SUPERVISION) $(PROJECT_OPERATIONS) $(PROJECT_ALERTS) $(PROJECT_LIFECYCLE) $(PACKAGE_INDEX) $(WORKER_COMMANDS) $(DEPLOYMENT_COMPOSE)",
        "",
    ]
    return "\n".join(lines)


def _init_project_relation_sample_files(
    *,
    pipeline_dir: Path,
    blueprints: list[ScaffoldBlueprint],
) -> list[Path]:
    created: list[Path] = []
    for blueprint in blueprints:
        additional_types = {
            document_type.id: document_type
            for document_type in blueprint.additional_document_types
        }
        for relation in blueprint.additional_document_relations:
            for target_document_type in relation.target_document_types:
                document_type = additional_types.get(target_document_type)
                if document_type is None:
                    continue
                media_type = _scaffold_sample_media_type(
                    list(document_type.media_types)
                    or list(blueprint.document_media_types)
                )
                document_id = _scaffold_sample_document_id(
                    media_type,
                    document_extensions=list(document_type.extensions)
                    or list(blueprint.document_extensions),
                )
                path = pipeline_dir / blueprint.id / "incoming" / document_id
                if path.exists():
                    continue
                created.append(
                    _write_new_file(
                        path,
                        _scaffold_sample_document_content(
                            document_id=document_id,
                            media_type=media_type,
                        ),
                    )
                )
    return created


def _init_project_source_list_csv(*, blueprints: list[ScaffoldBlueprint]) -> str:
    columns = [
        "document_id",
        "title",
        "path",
        "source_uri",
        "document_type",
        "relation",
        "parent_document_id",
        "parent_process_id",
        "media_type",
        "source_sha256",
        "value.source",
        "metadata.blueprint",
        "metadata.package_id",
    ]
    rows = []
    for blueprint in blueprints:
        media_type = _scaffold_sample_media_type(list(blueprint.document_media_types))
        sample_document_id = _scaffold_sample_document_id(
            media_type,
            document_extensions=list(blueprint.document_extensions),
        )
        document_id = f"{blueprint.id}_{sample_document_id}"
        incoming_path = f"pipelines/{blueprint.id}/incoming/{sample_document_id}"
        rows.append(
            {
                "document_id": document_id,
                "title": f"{blueprint.title} sample",
                "path": incoming_path,
                "source_uri": "",
                "document_type": blueprint.document_type,
                "relation": "",
                "parent_document_id": "",
                "parent_process_id": "",
                "media_type": media_type,
                "source_sha256": "",
                "value.source": document_id,
                "metadata.blueprint": blueprint.id,
                "metadata.package_id": blueprint.id,
            }
        )
        additional_types = {
            document_type.id: document_type
            for document_type in blueprint.additional_document_types
        }
        emitted_by_step = blueprint.emitted_document_types_by_step or {}
        for relation in blueprint.additional_document_relations:
            if (
                relation.source_document_types
                and blueprint.document_type not in relation.source_document_types
            ):
                continue
            for target_document_type in relation.target_document_types:
                document_type = additional_types.get(target_document_type)
                if document_type is None:
                    continue
                child_media_type = _scaffold_sample_media_type(
                    list(document_type.media_types)
                    or list(blueprint.document_media_types)
                )
                child_sample_document_id = _scaffold_sample_document_id(
                    child_media_type,
                    document_extensions=list(document_type.extensions)
                    or list(blueprint.document_extensions),
                )
                child_path = (
                    f"pipelines/{blueprint.id}/incoming/{child_sample_document_id}"
                )
                child_document_id = (
                    f"{blueprint.id}_{relation.id}_{child_sample_document_id}"
                )
                parent_process_id = next(
                    (
                        step_id
                        for step_id, emitted_types in emitted_by_step.items()
                        if target_document_type in emitted_types
                    ),
                    "",
                )
                rows.append(
                    {
                        "document_id": child_document_id,
                        "title": (
                            f"{blueprint.title} {relation.title or relation.id} sample"
                        ),
                        "path": child_path,
                        "source_uri": "",
                        "document_type": target_document_type,
                        "relation": relation.id,
                        "parent_document_id": document_id,
                        "parent_process_id": parent_process_id,
                        "media_type": child_media_type,
                        "source_sha256": "",
                        "value.source": child_document_id,
                        "metadata.blueprint": blueprint.id,
                        "metadata.package_id": blueprint.id,
                    }
                )
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def _init_project_routes_yaml(*, blueprints: list[ScaffoldBlueprint]) -> str:
    routes = []
    for blueprint in blueprints:
        routes.append(
            _init_project_document_route(
                blueprint=blueprint,
                route_id=blueprint.id,
                document_type=blueprint.document_type,
                media_types=list(blueprint.document_media_types),
                extensions=list(blueprint.document_extensions),
            )
        )
        for document_type in blueprint.additional_document_types:
            routes.append(
                _init_project_document_route(
                    blueprint=blueprint,
                    route_id=f"{blueprint.id}_{document_type.id}",
                    document_type=document_type.id,
                    media_types=list(document_type.media_types),
                    extensions=list(document_type.extensions),
                )
            )
    return yaml.safe_dump(
        {"routes": routes},
        sort_keys=False,
        allow_unicode=False,
    )


def _init_project_document_route(
    *,
    blueprint: ScaffoldBlueprint,
    route_id: str,
    document_type: str,
    media_types: list[str],
    extensions: list[str],
) -> dict[str, Any]:
    match: dict[str, Any] = {
        "document_types": [document_type],
    }
    if media_types:
        match["media_types"] = media_types
    if extensions:
        match["extensions"] = extensions
    return {
        "id": route_id,
        "match": match,
        "set": {
            "pipeline_id": f"{blueprint.id}_flow",
            "document_type": document_type,
            "metadata": {
                "blueprint": blueprint.id,
                "package_id": blueprint.id,
            },
        },
    }


def _scaffold_workflow_package(
    *,
    output_dir: Path,
    package_id: str,
    pipeline_id: str,
    steps: list[str],
    adapter_kind: str,
    title: str | None = None,
    document_type: str = "generic_document",
    document_media_types: list[str] | None = None,
    document_extensions: list[str] | None = None,
    document_value_schema: dict[str, Any] | None = None,
    document_metadata_schema: dict[str, Any] | None = None,
    additional_document_types: list[DocumentTypeSpec] | None = None,
    additional_document_relations: list[DocumentRelationSpec] | None = None,
    operation_types: list[OperationTypeSpec] | None = None,
    operation_type_by_step: dict[str, str] | None = None,
    needs_by_step: dict[str, list[str]] | None = None,
    artifact_kind_by_step: dict[str, str] | None = None,
    capability_by_step: dict[str, str] | None = None,
    accepted_document_types_by_step: dict[str, list[str]] | None = None,
    emitted_document_types_by_step: dict[str, list[str]] | None = None,
    artifact_media_types_by_step: dict[str, list[str]] | None = None,
    artifact_extensions_by_step: dict[str, list[str]] | None = None,
    artifact_value_schema_by_step: dict[str, dict[str, Any]] | None = None,
    capability_output_schema_by_step: dict[str, dict[str, Any]] | None = None,
    capability_streams_by_step: dict[str, list[StreamSpec]] | None = None,
    step_policy_by_step: dict[str, dict[str, Any]] | None = None,
    step_guidance_by_step: dict[str, dict[str, Any]] | None = None,
    blueprint_id: str | None = None,
) -> dict[str, Any]:
    media_types = document_media_types or ["application/octet-stream"]
    artifact_kind_by_step = artifact_kind_by_step or {
        step_id: _scaffold_artifact_kind_id(step_id)
        for step_id in steps
    }
    capability_by_step = capability_by_step or {
        step_id: _scaffold_capability_id(step_id)
        for step_id in steps
    }
    operation_type_overrides = operation_type_by_step or {}
    operation_type_by_step = {
        step_id: operation_type_overrides.get(step_id) or operation_type_for_step(step_id)
        for step_id in steps
    }
    _validate_scaffold_step_mapping("artifact kind", artifact_kind_by_step, steps)
    _validate_scaffold_step_mapping("capability", capability_by_step, steps)
    _validate_scaffold_step_mapping(
        "operation type",
        operation_type_by_step,
        steps,
    )
    _validate_scaffold_partial_step_mapping(
        "step needs",
        needs_by_step,
        steps,
    )
    _validate_scaffold_partial_step_mapping(
        "accepted document types",
        accepted_document_types_by_step,
        steps,
    )
    _validate_scaffold_partial_step_mapping(
        "emitted document types",
        emitted_document_types_by_step,
        steps,
    )
    _validate_scaffold_partial_step_mapping(
        "artifact media types",
        artifact_media_types_by_step,
        steps,
    )
    _validate_scaffold_partial_step_mapping(
        "artifact extensions",
        artifact_extensions_by_step,
        steps,
    )
    _validate_scaffold_partial_step_mapping(
        "artifact value schema",
        artifact_value_schema_by_step,
        steps,
    )
    _validate_scaffold_partial_step_mapping(
        "capability output schema",
        capability_output_schema_by_step,
        steps,
    )
    _validate_scaffold_partial_step_mapping(
        "capability streams",
        capability_streams_by_step,
        steps,
    )
    _validate_scaffold_partial_step_mapping(
        "step policy",
        step_policy_by_step,
        steps,
    )
    _validate_scaffold_partial_step_mapping(
        "step guidance",
        step_guidance_by_step,
        steps,
    )
    resolved_needs_by_step = {
        step_id: _scaffold_process_needs(
            step_id=step_id,
            index=index,
            steps=steps,
            needs_by_step=needs_by_step,
        )
        for index, step_id in enumerate(steps)
    }
    pipeline_steps = [
        _scaffold_process_spec(
            package_id=package_id,
            step_id=step_id,
            needs=resolved_needs_by_step[step_id],
            capability=capability_by_step[step_id],
            adapter_kind=adapter_kind,
            policy=(step_policy_by_step or {}).get(step_id),
        )
        for index, step_id in enumerate(steps)
    ]
    package_operation_types = _scaffold_operation_type_specs(
        steps=steps,
        operation_type_by_step=operation_type_by_step,
        operation_types=operation_types or [],
    )
    package = WorkflowPackageSpec(
        id=package_id,
        title=title or _title_from_id(package_id),
        version="1",
        document_types=[
            DocumentTypeSpec(
                id=document_type,
                title=_title_from_id(document_type),
                media_types=media_types,
                extensions=document_extensions or [],
                value_schema=json.loads(json.dumps(document_value_schema or {})),
                metadata_schema=json.loads(json.dumps(document_metadata_schema or {})),
            )
        ]
        + [
            DocumentTypeSpec.model_validate(
                document_type_spec.model_dump(mode="json")
            )
            for document_type_spec in additional_document_types or []
        ],
        document_relations=[
            DocumentRelationSpec.model_validate(
                relation.model_dump(mode="json")
            )
            for relation in additional_document_relations or []
        ],
        operation_types=package_operation_types,
        artifact_kinds=[
            ArtifactKindSpec(
                id=artifact_kind_by_step[step_id],
                title=f"{_title_from_id(step_id)} output",
                media_types=_scaffold_artifact_media_types(
                    step_id,
                    artifact_media_types_by_step=artifact_media_types_by_step,
                ),
                extensions=_scaffold_artifact_extensions(
                    step_id,
                    artifact_extensions_by_step=artifact_extensions_by_step,
                ),
                value_schema=_scaffold_artifact_value_schema(
                    step_id,
                    artifact_value_schema_by_step=artifact_value_schema_by_step,
                ),
            )
            for step_id in steps
        ],
        capabilities=[
            CapabilitySpec(
                id=capability_by_step[step_id],
                title=_title_from_id(capability_by_step[step_id]),
                operation_type=operation_type_by_step[step_id],
                accepts_document_types=_scaffold_capability_document_inputs(
                    step_id,
                    needs=resolved_needs_by_step[step_id],
                    default_document_type=document_type,
                    accepted_document_types_by_step=accepted_document_types_by_step,
                ),
                accepts_artifact_kinds=[
                    artifact_kind_by_step[need]
                    for need in resolved_needs_by_step[step_id]
                ],
                emits_document_types=list(
                    (emitted_document_types_by_step or {}).get(step_id, [])
                ),
                emits_artifact_kinds=[artifact_kind_by_step[step_id]],
                output_schema=_scaffold_capability_output_schema(
                    step_id,
                    capability_output_schema_by_step=capability_output_schema_by_step,
                ),
                emits_streams=_scaffold_capability_streams(
                    step_id,
                    capability_streams_by_step=capability_streams_by_step,
                ),
            )
            for index, step_id in enumerate(steps)
        ],
        pipelines=[f"{pipeline_id}.yaml"],
        workers=[
            WorkflowWorkerSpec(
                id=f"{step.id}_worker",
                title=f"{_title_from_id(step.id)} worker",
                capabilities=[capability_by_step[step.id]],
                pipeline_id=pipeline_id,
                process_id=step.id,
                command=["python", f"steps/{step.id}.py"],
                cwd=".",
            )
            for step in pipeline_steps
            if step.adapter.kind == "queue"
        ]
    )
    pipeline = PipelineSpec(
        id=pipeline_id,
        title=title or _title_from_id(pipeline_id),
        steps=pipeline_steps,
        combines=[
            CombineSpec(
                id="workflow_result",
                needs=steps,
                emit_partial=True,
            )
        ],
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    steps_dir = output_dir / "steps"
    steps_dir.mkdir(parents=True, exist_ok=True)
    sample_media_type = _scaffold_sample_media_type(media_types)
    sample_document_id = _scaffold_sample_document_id(
        sample_media_type,
        document_extensions=document_extensions or [],
    )

    created = [
        _write_new_file(
            output_dir / "process-runtime-package.yaml",
            _package_yaml(package),
        ),
        _write_new_file(
            output_dir / f"{pipeline_id}.yaml",
            _pipeline_yaml(pipeline),
        ),
        _write_new_file(
            output_dir / "run-input.example.yaml",
            _scaffold_run_input_yaml(
                package_id=package_id,
                pipeline_id=pipeline_id,
                run_id=f"run_{pipeline_id}_sample",
                document_type=document_type,
                document_media_types=media_types,
                document_extensions=document_extensions or [],
                document_value_schema=document_value_schema or {},
                document_metadata_schema=document_metadata_schema or {},
                source_uri=(output_dir / "incoming" / sample_document_id).resolve().as_uri(),
                blueprint_id=blueprint_id,
            ),
        ),
        _write_new_file(
            output_dir / "source-list.example.csv",
            _scaffold_source_list_csv(
                document_type=document_type,
                document_media_types=media_types,
                document_extensions=document_extensions or [],
                document_value_schema=document_value_schema or {},
                document_metadata_schema=document_metadata_schema or {},
            ),
        ),
        _write_new_file(
            output_dir / "incoming" / sample_document_id,
            _scaffold_sample_document_content(
                document_id=sample_document_id,
                media_type=sample_media_type,
            ),
        ),
        _write_new_file(
            output_dir / "README.scaffold.md",
            _scaffold_readme(
                run_id=f"run_{pipeline_id}_sample",
                package=package,
                pipeline=pipeline,
                step_guidance_by_step={
                    step_id: _scaffold_step_guidance(
                        step_id=step_id,
                        document_type=document_type,
                        capability=capability_by_step[step_id],
                        operation_type=operation_type_by_step[step_id],
                        artifact_kind=artifact_kind_by_step[step_id],
                        needs=resolved_needs_by_step[step_id],
                        accepts_document_types=_scaffold_capability_document_inputs(
                            step_id,
                            needs=resolved_needs_by_step[step_id],
                            default_document_type=document_type,
                            accepted_document_types_by_step=accepted_document_types_by_step,
                        ),
                        accepts_artifact_kinds=[
                            artifact_kind_by_step[need]
                            for need in resolved_needs_by_step[step_id]
                        ],
                        emits_document_types=list(
                            (emitted_document_types_by_step or {}).get(step_id, [])
                        ),
                        streams=_scaffold_capability_streams(
                            step_id,
                            capability_streams_by_step=capability_streams_by_step,
                        ),
                        guidance=(step_guidance_by_step or {}).get(step_id),
                    )
                    for step_id in steps
                },
            ),
        ),
        _write_new_file(
            output_dir / "Makefile",
            _scaffold_makefile(
                run_id=f"run_{pipeline_id}_sample",
                package=package,
                pipeline=pipeline,
            ),
        ),
    ]
    for step_id in steps:
        created.append(
            _write_new_file(
                steps_dir / f"{step_id}.py",
                _step_program_source(
                    step_id=step_id,
                    artifact_kind=artifact_kind_by_step[step_id],
                    artifact_value_schema=_scaffold_artifact_value_schema(
                        step_id,
                        artifact_value_schema_by_step=artifact_value_schema_by_step,
                    ),
                    output_schema=_scaffold_capability_output_schema(
                        step_id,
                        capability_output_schema_by_step=capability_output_schema_by_step,
                    ),
                    streams=_scaffold_capability_streams(
                        step_id,
                        capability_streams_by_step=capability_streams_by_step,
                    ),
                    guidance=_scaffold_step_guidance(
                        step_id=step_id,
                        document_type=document_type,
                        capability=capability_by_step[step_id],
                        operation_type=operation_type_by_step[step_id],
                        artifact_kind=artifact_kind_by_step[step_id],
                        needs=resolved_needs_by_step[step_id],
                        accepts_document_types=_scaffold_capability_document_inputs(
                            step_id,
                            needs=resolved_needs_by_step[step_id],
                            default_document_type=document_type,
                            accepted_document_types_by_step=accepted_document_types_by_step,
                        ),
                        accepts_artifact_kinds=[
                            artifact_kind_by_step[need]
                            for need in resolved_needs_by_step[step_id]
                        ],
                        emits_document_types=list(
                            (emitted_document_types_by_step or {}).get(step_id, [])
                        ),
                        streams=_scaffold_capability_streams(
                            step_id,
                            capability_streams_by_step=capability_streams_by_step,
                        ),
                        guidance=(step_guidance_by_step or {}).get(step_id),
                    ),
                ),
            )
        )
    created.extend(
        _scaffold_contract_files(
            output_dir=output_dir,
            package=package,
            pipeline=pipeline,
        )
    )

    return {
        "ok": True,
        "blueprint": blueprint_id,
        "package_id": package_id,
        "pipeline_id": pipeline_id,
        "adapter_kind": adapter_kind,
        "document_type": document_type,
        "step_ids": steps,
        "needs_by_step": {step.id: list(step.needs) for step in pipeline.steps},
        "created": [str(path) for path in created],
    }


def _scaffold_run_input_yaml(
    *,
    package_id: str,
    pipeline_id: str,
    run_id: str,
    document_type: str,
    document_media_types: list[str],
    document_extensions: list[str],
    document_value_schema: dict[str, Any],
    document_metadata_schema: dict[str, Any],
    source_uri: str,
    blueprint_id: str | None,
) -> str:
    media_type = _scaffold_sample_media_type(document_media_types)
    document_id = _scaffold_sample_document_id(
        media_type,
        document_extensions=document_extensions,
    )
    metadata = {
        "package_id": package_id,
    }
    if blueprint_id is not None:
        metadata["blueprint"] = blueprint_id
    payload = {
        "run_id": run_id,
        "pipeline_id": pipeline_id,
        "title": f"Sample {pipeline_id} run",
        "metadata": metadata,
        "documents": [
            {
                "document_id": document_id,
                "title": "Sample document",
                "document_type": document_type,
                "media_type": media_type,
                "source_uri": source_uri,
                "values": {
                    "source": document_id,
                    **_scaffold_schema_sample_values(
                        document_value_schema,
                        document_id=document_id,
                    ),
                },
                "metadata": {
                    "sample": True,
                    **_scaffold_schema_sample_values(
                        document_metadata_schema,
                        document_id=document_id,
                    ),
                },
            }
        ],
    }
    return yaml.safe_dump(payload, sort_keys=False)


def _scaffold_source_list_csv(
    *,
    document_type: str,
    document_media_types: list[str],
    document_extensions: list[str],
    document_value_schema: dict[str, Any],
    document_metadata_schema: dict[str, Any],
) -> str:
    media_type = _scaffold_sample_media_type(document_media_types)
    document_id = _scaffold_sample_document_id(
        media_type,
        document_extensions=document_extensions,
    )
    value_keys = _scaffold_schema_property_keys(document_value_schema)
    metadata_keys = _scaffold_schema_property_keys(document_metadata_schema)
    columns = [
        "document_id",
        "title",
        "path",
        "source_uri",
        "document_type",
        "relation",
        "parent_document_id",
        "parent_process_id",
        "media_type",
        "source_sha256",
        *[f"value.{key}" for key in value_keys],
        *[f"metadata.{key}" for key in metadata_keys],
    ]
    row: dict[str, str] = {
        "document_id": document_id,
        "title": "Sample document",
        "path": f"incoming/{document_id}",
        "source_uri": "",
        "document_type": document_type,
        "relation": "",
        "parent_document_id": "",
        "parent_process_id": "",
        "media_type": media_type,
        "source_sha256": "",
    }
    value_properties = document_value_schema.get("properties") or {}
    for key in value_keys:
        schema = (
            value_properties.get(key)
            if isinstance(value_properties, dict)
            else {}
        )
        row[f"value.{key}"] = _scaffold_source_list_value(
            key,
            schema if isinstance(schema, dict) else {},
            document_id=document_id,
        )
    metadata_properties = document_metadata_schema.get("properties") or {}
    for key in metadata_keys:
        schema = (
            metadata_properties.get(key)
            if isinstance(metadata_properties, dict)
            else {}
        )
        row[f"metadata.{key}"] = _scaffold_source_list_value(
            key,
            schema if isinstance(schema, dict) else {},
            document_id=document_id,
        )

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerow(row)
    return buffer.getvalue()


def _scaffold_contract_files(
    *,
    output_dir: Path,
    package: WorkflowPackageSpec,
    pipeline: PipelineSpec,
) -> list[Path]:
    created: list[Path] = []
    contract_dir = output_dir / "contracts"
    for document_type in package.document_types:
        if document_type.value_schema:
            created.append(
                _write_contract_yaml(
                    contract_dir
                    / "documents"
                    / f"{document_type.id}.values.schema.yaml",
                    document_type.value_schema,
                )
            )
        if document_type.metadata_schema:
            created.append(
                _write_contract_yaml(
                    contract_dir
                    / "documents"
                    / f"{document_type.id}.metadata.schema.yaml",
                    document_type.metadata_schema,
                )
            )
    for artifact_kind in package.artifact_kinds:
        if artifact_kind.value_schema:
            created.append(
                _write_contract_yaml(
                    contract_dir
                    / "artifacts"
                    / f"{artifact_kind.id}.value.schema.yaml",
                    artifact_kind.value_schema,
                )
            )
        if artifact_kind.metadata_schema:
            created.append(
                _write_contract_yaml(
                    contract_dir
                    / "artifacts"
                    / f"{artifact_kind.id}.metadata.schema.yaml",
                    artifact_kind.metadata_schema,
                )
            )
    for capability in package.capabilities:
        if capability.config_schema:
            created.append(
                _write_contract_yaml(
                    contract_dir
                    / "capabilities"
                    / f"{capability.id}.config.schema.yaml",
                    capability.config_schema,
                )
            )
        if capability.output_schema:
            created.append(
                _write_contract_yaml(
                    contract_dir
                    / "capabilities"
                    / f"{capability.id}.output.schema.yaml",
                    capability.output_schema,
                )
            )
        if capability.emits_streams:
            created.append(
                _write_contract_yaml(
                    contract_dir / "streams" / f"{capability.id}.streams.yaml",
                    {
                        "streams": [
                            stream.model_dump(mode="json", by_alias=True)
                            for stream in capability.emits_streams
                        ]
                    },
                )
            )
    for step in pipeline.steps:
        policy = _scaffold_step_policy_template(step)
        if policy:
            created.append(
                _write_contract_yaml(
                    contract_dir / "policies" / f"{step.id}.policy.yaml",
                    policy,
                )
            )
    return created


def _write_contract_yaml(path: Path, value: dict[str, Any]) -> Path:
    return _write_new_file(
        path,
        yaml.safe_dump(value, sort_keys=False),
    )


def _scaffold_step_policy_template(step: ProcessSpec) -> dict[str, Any]:
    policy: dict[str, Any] = {}
    if step.title:
        policy["title"] = step.title
    if step.description:
        policy["description"] = step.description
    if step.tags:
        policy["tags"] = list(step.tags)
    if step.timeout_seconds is not None:
        policy["timeout_seconds"] = step.timeout_seconds
    if step.priority:
        policy["priority"] = step.priority
    if step.max_concurrency is not None:
        policy["max_concurrency"] = step.max_concurrency
    if step.resource_pool != "default":
        policy["resource_pool"] = step.resource_pool
    resources = step.resources.model_dump(
        mode="json",
        exclude_defaults=True,
        exclude_none=True,
    )
    if resources:
        policy["resources"] = resources
    retry = step.retry.model_dump(
        mode="json",
        exclude_defaults=True,
        exclude_none=True,
    )
    if retry:
        policy["retry"] = retry
    sla = step.sla.model_dump(
        mode="json",
        exclude_defaults=True,
        exclude_none=True,
    )
    if sla:
        policy["sla"] = sla
    when = step.when.model_dump(
        mode="json",
        exclude_defaults=True,
        exclude_none=True,
    )
    if when:
        policy["when"] = when
    if step.wait_for_children is not None:
        policy["wait_for_children"] = step.wait_for_children.model_dump(
            mode="json",
            exclude_defaults=True,
            exclude_none=True,
        )
    if step.config:
        policy["config"] = dict(step.config)
    if step.adapter.kind in {"http", "manual"}:
        policy["adapter"] = step.adapter.model_dump(
            mode="json",
            exclude_defaults=True,
            exclude_none=True,
        )
        policy["adapter"]["kind"] = step.adapter.kind
    return policy


def _sync_contracts_command(args: argparse.Namespace) -> dict[str, Any]:
    package_yaml = Path(args.package_yaml)
    pipeline_yaml = Path(args.pipeline_yaml)
    contract_dir = Path(args.contract_dir)
    package = _load_package_yaml_for_sync(package_yaml)
    pipeline = _load_pipeline_yaml_for_sync(pipeline_yaml)
    package = _apply_contract_dir_to_package(package, contract_dir=contract_dir)
    pipeline = _apply_contract_dir_to_pipeline(pipeline, contract_dir=contract_dir)
    package = _sync_package_workers_with_pipeline(package, pipeline)
    package_yaml.write_text(_package_yaml(package), encoding="utf-8")
    pipeline_yaml.write_text(_pipeline_yaml(pipeline), encoding="utf-8")
    return {
        "ok": True,
        "package_id": package.id,
        "pipeline_id": pipeline.id,
        "updated": [str(package_yaml), str(pipeline_yaml)],
    }


def _load_package_yaml_for_sync(path: Path) -> WorkflowPackageSpec:
    data = _read_yaml_object(str(path), label="workflow package")
    return workflow_package_from_mapping(data)


def _load_pipeline_yaml_for_sync(path: Path) -> PipelineSpec:
    data = _read_yaml_object(str(path), label="pipeline")
    return pipeline_from_mapping(data)


def _apply_contract_dir_to_package(
    package: WorkflowPackageSpec,
    *,
    contract_dir: Path,
) -> WorkflowPackageSpec:
    document_types = [
        document_type.model_copy(
            update={
                "value_schema": _sync_optional_contract_object(
                    contract_dir
                    / "documents"
                    / f"{document_type.id}.values.schema.yaml",
                    fallback=document_type.value_schema,
                    label="document value schema",
                ),
                "metadata_schema": _sync_optional_contract_object(
                    contract_dir
                    / "documents"
                    / f"{document_type.id}.metadata.schema.yaml",
                    fallback=document_type.metadata_schema,
                    label="document metadata schema",
                ),
            }
        )
        for document_type in package.document_types
    ]
    artifact_kinds = [
        artifact_kind.model_copy(
            update={
                "value_schema": _sync_optional_contract_object(
                    contract_dir
                    / "artifacts"
                    / f"{artifact_kind.id}.value.schema.yaml",
                    fallback=artifact_kind.value_schema,
                    label="artifact value schema",
                ),
                "metadata_schema": _sync_optional_contract_object(
                    contract_dir
                    / "artifacts"
                    / f"{artifact_kind.id}.metadata.schema.yaml",
                    fallback=artifact_kind.metadata_schema,
                    label="artifact metadata schema",
                ),
            }
        )
        for artifact_kind in package.artifact_kinds
    ]
    capabilities = []
    for capability in package.capabilities:
        streams_path = contract_dir / "streams" / f"{capability.id}.streams.yaml"
        capabilities.append(
            capability.model_copy(
                update={
                    "config_schema": _sync_optional_contract_object(
                        contract_dir
                        / "capabilities"
                        / f"{capability.id}.config.schema.yaml",
                        fallback=capability.config_schema,
                        label="capability config schema",
                    ),
                    "output_schema": _sync_optional_contract_object(
                        contract_dir
                        / "capabilities"
                        / f"{capability.id}.output.schema.yaml",
                        fallback=capability.output_schema,
                        label="capability output schema",
                    ),
                    "emits_streams": (
                        _coerce_scaffold_stream_specs(
                            _read_yaml_value(str(streams_path), label="stream contract"),
                            source=str(streams_path),
                        )
                        if streams_path.exists()
                        else capability.emits_streams
                    ),
                }
            )
        )
    return package.model_copy(
        update={
            "document_types": document_types,
            "artifact_kinds": artifact_kinds,
            "capabilities": capabilities,
        }
    )


def _apply_contract_dir_to_pipeline(
    pipeline: PipelineSpec,
    *,
    contract_dir: Path,
) -> PipelineSpec:
    steps = []
    for step in pipeline.steps:
        policy_path = contract_dir / "policies" / f"{step.id}.policy.yaml"
        if policy_path.exists():
            policy = _read_yaml_object(str(policy_path), label="step policy")
            invalid = sorted(set(policy) - _SCAFFOLD_STEP_POLICY_FIELDS)
            if invalid:
                raise ValueError(
                    f"Invalid scaffold step policy {policy_path!s}: unsupported "
                    f"{', '.join(invalid)}"
                )
            base_payload = step.model_dump(mode="json", by_alias=True)
            if "adapter" in policy:
                base_payload["adapter"] = policy["adapter"]
                policy = {
                    key: value
                    for key, value in policy.items()
                    if key != "adapter"
                }
            payload = _deep_merge_scaffold_policy(base_payload, policy)
            steps.append(ProcessSpec.model_validate(payload))
        else:
            steps.append(step)
    return pipeline.model_copy(update={"steps": steps})


def _sync_optional_contract_object(
    path: Path,
    *,
    fallback: dict[str, Any],
    label: str,
) -> dict[str, Any]:
    if not path.exists():
        return fallback
    return _read_yaml_object(str(path), label=label)


def _sync_package_workers_with_pipeline(
    package: WorkflowPackageSpec,
    pipeline: PipelineSpec,
) -> WorkflowPackageSpec:
    steps_by_id = {step.id: step for step in pipeline.steps}
    workers: list[WorkflowWorkerSpec] = []
    existing_by_process: dict[str, WorkflowWorkerSpec] = {}
    for worker in package.workers:
        if worker.pipeline_id != pipeline.id or worker.process_id not in steps_by_id:
            workers.append(worker)
            continue
        step = steps_by_id[worker.process_id or ""]
        if step.adapter.kind != "queue":
            continue
        existing_by_process[step.id] = worker
    for step in pipeline.steps:
        if step.adapter.kind != "queue":
            continue
        existing = existing_by_process.get(step.id)
        capabilities = [step.capability] if step.capability else []
        if existing is not None:
            workers.append(
                existing.model_copy(
                    update={
                        "capabilities": capabilities,
                        "adapter_kind": "queue",
                    }
                )
            )
            continue
        workers.append(
            WorkflowWorkerSpec(
                id=f"{step.id}_worker",
                title=f"{_title_from_id(step.id)} worker",
                capabilities=capabilities,
                pipeline_id=pipeline.id,
                process_id=step.id,
                command=["python", f"steps/{step.id}.py"],
                cwd=".",
            )
        )
    return package.model_copy(update={"workers": workers})


def _scaffold_schema_property_keys(schema: dict[str, Any]) -> list[str]:
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        return []
    return [str(key) for key in properties]


def _scaffold_schema_sample_values(
    schema: dict[str, Any],
    *,
    document_id: str,
) -> dict[str, Any]:
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        return {}
    values: dict[str, Any] = {}
    for key, property_schema in properties.items():
        key = str(key)
        values[key] = _scaffold_schema_sample_value(
            key,
            property_schema if isinstance(property_schema, dict) else {},
            document_id=document_id,
        )
    return values


def _scaffold_source_list_value(
    key: str,
    schema: dict[str, Any],
    *,
    document_id: str,
) -> str:
    value = _scaffold_schema_sample_value(key, schema, document_id=document_id)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return str(value)


def _scaffold_schema_sample_value(
    key: str,
    schema: dict[str, Any],
    *,
    document_id: str,
) -> Any:
    if key == "source":
        return document_id
    if key == "document_id":
        return document_id
    if key.endswith("_id"):
        return f"sample-{key}"
    kind = schema.get("type")
    if isinstance(kind, list):
        kind = next((item for item in kind if item != "null"), "string")
    if kind == "boolean":
        return True
    if kind == "integer":
        return 1
    if kind == "number":
        return 1.0
    if kind == "array":
        return []
    if kind == "object":
        return {}
    return f"sample {key.replace('_', ' ')}"


def _scaffold_sample_media_type(media_types: list[str]) -> str:
    preferred = [
        media_type
        for media_type in media_types
        if media_type != "application/octet-stream"
    ]
    media_type = (preferred or media_types or ["application/octet-stream"])[0]
    wildcard_samples = {
        "image/*": "image/png",
        "audio/*": "audio/wav",
        "video/*": "video/mp4",
        "text/*": "text/plain",
    }
    return wildcard_samples.get(media_type, media_type)


def _scaffold_sample_document_id(
    media_type: str,
    *,
    document_extensions: list[str] | None = None,
) -> str:
    extension_by_media_type = {
        "application/pdf": ".pdf",
        "application/json": ".json",
        "application/xml": ".xml",
        "application/zip": ".zip",
        "message/rfc822": ".eml",
        "text/csv": ".csv",
        "text/html": ".html",
        "text/markdown": ".md",
        "text/plain": ".txt",
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "audio/mpeg": ".mp3",
        "audio/wav": ".wav",
        "video/mp4": ".mp4",
    }
    extension = (document_extensions or [None])[0]
    if extension is None:
        extension = extension_by_media_type.get(media_type)
    if extension is None:
        guessed = mimetypes.guess_extension(media_type)
        extension = guessed if guessed not in (None, ".jpe") else ".bin"
    return f"sample{extension}"


def _scaffold_sample_document_content(
    *,
    document_id: str,
    media_type: str,
) -> str:
    if media_type == "application/json":
        return json.dumps(
            {
                "document_id": document_id,
                "sample": True,
                "text": "Sample Fala document",
            },
            indent=2,
            sort_keys=True,
        ) + "\n"
    if media_type == "message/rfc822":
        return (
            "From: sender@example.com\n"
            "To: recipient@example.com\n"
            "Subject: Sample Fala document\n"
            "\n"
            "Sample message body for Fala bootstrap.\n"
        )
    if media_type == "text/markdown":
        return "# Sample Fala document\n\nGenerated scaffold input.\n"
    if media_type == "text/csv":
        return "id,text\nsample,Sample Fala document\n"
    if media_type.startswith("text/"):
        return "Sample Fala document\n"
    return (
        f"Sample Fala document: {document_id}\n"
        f"Media type: {media_type}\n"
    )


def _scaffold_readme(
    *,
    run_id: str,
    package: WorkflowPackageSpec,
    pipeline: PipelineSpec,
    step_guidance_by_step: dict[str, dict[str, Any]] | None = None,
) -> str:
    package_id = package.id
    pipeline_id = pipeline.id
    first_worker_id = package.workers[0].id if package.workers else "worker"
    manual_steps = [step for step in pipeline.steps if step.adapter.kind == "manual"]
    subprocess_steps = [
        step for step in pipeline.steps if step.adapter.kind == "subprocess"
    ]
    queue_steps = [step for step in pipeline.steps if step.adapter.kind == "queue"]
    worker_section = (
        "\n".join(
            [
                "Run generated subprocess steps locally:",
                "",
                "```bash",
                "uv run fala --pipeline-dir . run-until-idle \\",
                "  --db runtime.db \\",
                f"  --pipeline {pipeline_id} \\",
                f"  --run-id {run_id} \\",
                "  --worker-id local-worker \\",
                "  --adapter-kind subprocess",
                "```",
            ]
        )
        if subprocess_steps
        else "\n".join(
            [
                "Run a declared package worker against a running Fala API:",
                "",
                "```bash",
                "uv run process-runtime-worker \\",
                "  --pipeline-dir . \\",
                f"  --package-id {package_id} \\",
                f"  --package-worker {first_worker_id} \\",
                "  --base-url http://localhost:8000 \\",
                f"  --run-id {run_id}",
                "```",
            ]
        )
        if queue_steps
        else "No automatic workers are declared. Complete manual steps through API, CLI, or web panel."
    )
    manual_section = ""
    if manual_steps:
        first_manual = manual_steps[0]
        manual_section = (
            "\n"
            "Complete a manual gate:\n"
            "\n"
            "```bash\n"
            "uv run fala --pipeline-dir . complete-process \\\n"
            "  --db runtime.db \\\n"
            f"  --pipeline {pipeline_id} \\\n"
            f"  --run-id {run_id} \\\n"
            "  --document-id sample.document \\\n"
            f"  --process-id {first_manual.id} \\\n"
            "  --value approved=true\n"
            "```\n"
        )
    return (
        f"# {_title_from_id(package_id)} scaffold\n"
        "\n"
        "Generated by `fala scaffold`.\n"
        "\n"
        f"{_scaffold_contract_summary(package)}"
        "\n"
        f"{_scaffold_guidance_summary(pipeline, step_guidance_by_step or {})}"
        "\n"
        f"{_scaffold_policy_summary(pipeline)}"
        "\n"
        "Validate package wiring and the sample run input:\n"
        "\n"
        "```bash\n"
        "uv run fala --pipeline-dir . validate --json --check-commands\n"
        "uv run fala --pipeline-dir . package-doctor\n"
        "uv run fala --pipeline-dir . validate-run --run-input run-input.example.yaml\n"
        "```\n"
        "\n"
        "Create the sample run:\n"
        "\n"
        "```bash\n"
        "uv run fala --pipeline-dir . create-run \\\n"
        "  --db runtime.db \\\n"
        "  --run-input run-input.example.yaml\n"
        "```\n"
        "\n"
        "Create a batch manifest from `source-list.example.csv`:\n"
        "\n"
        "```bash\n"
        "uv run fala discover-documents \\\n"
        "  --source-list source-list.example.csv \\\n"
        f"  --pipeline {pipeline_id} \\\n"
        f"  --run-id {run_id} \\\n"
        "  > run-input.from-source-list.json\n"
        "```\n"
        "\n"
        "After editing `contracts/`, sync them back into runtime YAML:\n"
        "\n"
        "```bash\n"
        "uv run fala sync-contracts \\\n"
        "  --package-yaml process-runtime-package.yaml \\\n"
        f"  --pipeline-yaml {pipeline_id}.yaml \\\n"
        "  --contract-dir contracts\n"
        "```\n"
        "\n"
        f"{worker_section}\n"
        f"{manual_section}"
        "\n"
        "Generated step programs live in `steps/`. Replace them with real work while "
        "keeping the same ProcessExecutionContext input and ProcessOutput output. "
        "Document, artifact, capability, output, and stream contracts live in "
        "`process-runtime-package.yaml`; editable copies live in `contracts/`. "
        "Step policy such as manual gates, retry, resources, and routing lives in "
        "the pipeline YAML; editable policy copies live in `contracts/policies/`. "
        "Stream-capable steps include sample `stream_chunk(...)` output; committed "
        "consumer checkpoints drive backpressure.\n"
    )


def _scaffold_makefile(
    *,
    run_id: str,
    package: WorkflowPackageSpec,
    pipeline: PipelineSpec,
) -> str:
    package_id = package.id
    pipeline_id = pipeline.id
    first_worker_id = package.workers[0].id if package.workers else ""
    has_subprocess = any(step.adapter.kind == "subprocess" for step in pipeline.steps)
    has_queue = any(step.adapter.kind == "queue" for step in pipeline.steps)
    lines = [
        "FALA ?= fala",
        "WORKER ?= process-runtime-worker",
        "DB ?= runtime.db",
        "PIPELINE_DIR ?= .",
        "RUN_INPUT ?= run-input.example.yaml",
        "SOURCE_LIST ?= source-list.example.csv",
        "SOURCE_LIST_RUN_INPUT ?= run-input.from-source-list.json",
        f"RUN_ID ?= {run_id}",
        "WORKER_ID ?= local-worker",
        "BASE_URL ?= http://localhost:8000",
        "",
        ".PHONY: bootstrap validate doctor validate-run source-list create run-local serve worker worker-commands sync-contracts clean",
        "",
        "bootstrap: validate doctor validate-run source-list",
        "",
        "validate:",
        "\t$(FALA) --pipeline-dir $(PIPELINE_DIR) validate --json --check-commands",
        "",
        "doctor:",
        "\t$(FALA) --pipeline-dir $(PIPELINE_DIR) package-doctor",
        "",
        "validate-run:",
        "\t$(FALA) --pipeline-dir $(PIPELINE_DIR) validate-run --run-input $(RUN_INPUT)",
        "",
        "source-list:",
        "\t$(FALA) discover-documents \\",
        "\t  --source-list $(SOURCE_LIST) \\",
        f"\t  --pipeline {pipeline_id} \\",
        "\t  --run-id $(RUN_ID) \\",
        "\t  > $(SOURCE_LIST_RUN_INPUT)",
        "",
        "create:",
        "\t$(FALA) --pipeline-dir $(PIPELINE_DIR) create-run \\",
        "\t  --db $(DB) \\",
        "\t  --run-input $(RUN_INPUT)",
        "",
        "sync-contracts:",
        "\t$(FALA) sync-contracts \\",
        "\t  --package-yaml process-runtime-package.yaml \\",
        f"\t  --pipeline-yaml {pipeline_id}.yaml \\",
        "\t  --contract-dir contracts",
        "",
        "serve:",
        "\t$(FALA) --pipeline-dir $(PIPELINE_DIR) serve --db $(DB)",
        "",
    ]
    if has_subprocess:
        lines.extend(
            [
                "run-local:",
                "\t$(FALA) --pipeline-dir $(PIPELINE_DIR) run-until-idle \\",
                "\t  --db $(DB) \\",
                f"\t  --pipeline {pipeline_id} \\",
                "\t  --run-id $(RUN_ID) \\",
                "\t  --worker-id $(WORKER_ID) \\",
                "\t  --adapter-kind subprocess",
                "",
            ]
        )
    else:
        lines.extend(
            [
                "run-local:",
                "\t@echo \"No subprocess steps declared. Use make serve and make worker.\"",
                "",
            ]
        )
    if has_queue and first_worker_id:
        lines.extend(
            [
                "worker:",
                "\t$(WORKER) \\",
                "\t  --pipeline-dir $(PIPELINE_DIR) \\",
                f"\t  --package-id {package_id} \\",
                f"\t  --package-worker {first_worker_id} \\",
                "\t  --base-url $(BASE_URL) \\",
                "\t  --run-id $(RUN_ID)",
                "",
                "worker-commands:",
                "\t$(FALA) --pipeline-dir $(PIPELINE_DIR) worker-commands \\",
                "\t  --base-url $(BASE_URL) \\",
                "\t  --run-id $(RUN_ID) \\",
                f"\t  --package-id {package_id}",
                "",
            ]
        )
    else:
        lines.extend(
            [
                "worker:",
                "\t@echo \"No queue package workers declared.\"",
                "",
                "worker-commands:",
                "\t@echo \"No queue package workers declared.\"",
                "",
            ]
        )
    lines.extend(
        [
            "clean:",
            "\trm -f $(DB) $(SOURCE_LIST_RUN_INPUT)",
            "",
        ]
    )
    return "\n".join(lines)


def _scaffold_contract_summary(package: WorkflowPackageSpec) -> str:
    document_types = ", ".join(item.id for item in package.document_types) or "none"
    operation_types = ", ".join(item.id for item in package.operation_types) or "none"
    artifact_kinds = ", ".join(item.id for item in package.artifact_kinds) or "none"
    capabilities = ", ".join(item.id for item in package.capabilities) or "none"
    stream_count = sum(len(capability.emits_streams) for capability in package.capabilities)
    return (
        "## Contract Surface\n"
        "\n"
        f"- Document types: {document_types}\n"
        f"- Operation types: {operation_types}\n"
        f"- Artifact kinds: {artifact_kinds}\n"
        f"- Capabilities: {capabilities}\n"
        f"- Stream contracts: {stream_count}\n"
    )


def _scaffold_guidance_summary(
    pipeline: PipelineSpec,
    step_guidance_by_step: dict[str, dict[str, Any]],
) -> str:
    lines = [
        "## Worker Guidance",
        "",
        "| Step | Role | Intent | Outputs |",
        "| --- | --- | --- | --- |",
    ]
    for step in pipeline.steps:
        guidance = step_guidance_by_step.get(step.id) or {}
        outputs = guidance.get("outputs") if isinstance(guidance.get("outputs"), dict) else {}
        output_bits = [str(outputs.get("artifact_kind") or "-")]
        streams = outputs.get("streams") if isinstance(outputs, dict) else []
        if streams:
            output_bits.append("streams: " + ", ".join(str(item) for item in streams))
        lines.append(
            "| "
            + " | ".join(
                [
                    step.id,
                    str(guidance.get("role") or _title_from_id(step.id)),
                    str(guidance.get("intent") or "-"),
                    ", ".join(output_bits),
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def _scaffold_policy_summary(pipeline: PipelineSpec) -> str:
    lines = [
        "## Step Policy",
        "",
        "| Step | Adapter | Capability | Needs | Priority | Concurrency | Resource pool |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for step in pipeline.steps:
        needs = ", ".join(step.needs) if step.needs else "-"
        concurrency = str(step.max_concurrency) if step.max_concurrency is not None else "-"
        lines.append(
            "| "
            + " | ".join(
                [
                    step.id,
                    step.adapter.kind,
                    step.capability or "-",
                    needs,
                    str(step.priority),
                    concurrency,
                    step.resource_pool,
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def _scaffold_capability_id(step_id: str) -> str:
    return f"{step_id}_document"


def _scaffold_artifact_kind_id(step_id: str) -> str:
    return f"{step_id}_output"


def _scaffold_operation_type_specs(
    *,
    steps: list[str],
    operation_type_by_step: dict[str, str],
    operation_types: list[OperationTypeSpec],
) -> list[OperationTypeSpec]:
    declared = {
        operation.id: OperationTypeSpec.model_validate(
            operation.model_dump(mode="json")
        )
        for operation in operation_types
    }
    specs: list[OperationTypeSpec] = []
    seen: set[str] = set()
    for step_id in steps:
        operation_type = operation_type_by_step[step_id]
        if operation_type in seen:
            continue
        seen.add(operation_type)
        specs.append(declared.get(operation_type) or operation_type_spec(operation_type))
    for operation in operation_types:
        if operation.id in seen:
            continue
        seen.add(operation.id)
        specs.append(
            OperationTypeSpec.model_validate(operation.model_dump(mode="json"))
        )
    return specs


def _scaffold_artifact_media_types(
    step_id: str,
    *,
    artifact_media_types_by_step: dict[str, list[str]] | None,
) -> list[str]:
    if artifact_media_types_by_step is None:
        return ["application/json"]
    return list(artifact_media_types_by_step.get(step_id, ["application/json"]))


def _scaffold_artifact_extensions(
    step_id: str,
    *,
    artifact_extensions_by_step: dict[str, list[str]] | None,
) -> list[str]:
    if artifact_extensions_by_step is None:
        return []
    return list(artifact_extensions_by_step.get(step_id, []))


def _scaffold_capability_document_inputs(
    step_id: str,
    *,
    needs: list[str],
    default_document_type: str,
    accepted_document_types_by_step: dict[str, list[str]] | None,
) -> list[str]:
    if (
        accepted_document_types_by_step is not None
        and step_id in accepted_document_types_by_step
    ):
        return list(accepted_document_types_by_step[step_id])
    return [default_document_type] if not needs else []


def _scaffold_artifact_value_schema(
    step_id: str,
    *,
    artifact_value_schema_by_step: dict[str, dict[str, Any]] | None,
) -> dict[str, Any]:
    if artifact_value_schema_by_step is not None and step_id in artifact_value_schema_by_step:
        return json.loads(json.dumps(artifact_value_schema_by_step[step_id]))
    return _scaffold_step_result_schema(step_id)


def _scaffold_capability_output_schema(
    step_id: str,
    *,
    capability_output_schema_by_step: dict[str, dict[str, Any]] | None,
) -> dict[str, Any]:
    if (
        capability_output_schema_by_step is not None
        and step_id in capability_output_schema_by_step
    ):
        return json.loads(json.dumps(capability_output_schema_by_step[step_id]))
    return _scaffold_step_result_schema(step_id)


def _scaffold_capability_streams(
    step_id: str,
    *,
    capability_streams_by_step: dict[str, list[StreamSpec]] | None,
) -> list[StreamSpec]:
    if capability_streams_by_step is None:
        return []
    return [
        StreamSpec.model_validate(stream.model_dump(mode="json", by_alias=True))
        for stream in capability_streams_by_step.get(step_id, [])
    ]


def _scaffold_step_result_schema(step_id: str) -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["status", "process_id", "document_id"],
        "properties": {
            "status": {"const": "ok"},
            "process_id": {"const": step_id},
            "document_id": {"type": ["string", "null"]},
        },
        "additionalProperties": True,
    }


def _validate_scaffold_step_mapping(
    label: str,
    mapping: dict[str, str],
    steps: list[str],
) -> None:
    expected = set(steps)
    actual = set(mapping)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        parts = []
        if missing:
            parts.append(f"missing {', '.join(missing)}")
        if extra:
            parts.append(f"extra {', '.join(extra)}")
        raise ValueError(f"Invalid scaffold {label} map: {'; '.join(parts)}")
    for value in mapping.values():
        ProcessSpec(id=value, adapter={"kind": "queue", "queue": "validate.id"})


def _validate_scaffold_partial_step_mapping(
    label: str,
    mapping: dict[str, Any] | None,
    steps: list[str],
) -> None:
    if mapping is None:
        return
    extra = sorted(set(mapping) - set(steps))
    if extra:
        raise ValueError(f"Invalid scaffold {label} map: extra {', '.join(extra)}")


def _scaffold_adapter(package_id: str, step_id: str, adapter_kind: str) -> dict[str, Any]:
    if adapter_kind == "subprocess":
        return {
            "kind": "subprocess",
            "command": ["python", f"steps/{step_id}.py"],
            "cwd": ".",
        }
    if adapter_kind == "queue":
        return {
            "kind": "queue",
            "queue": f"{package_id}.{step_id}",
        }
    raise ValueError(f"Unsupported scaffold adapter kind: {adapter_kind}")


def _scaffold_process_spec(
    *,
    package_id: str,
    step_id: str,
    needs: list[str],
    capability: str,
    adapter_kind: str,
    policy: dict[str, Any] | None,
) -> ProcessSpec:
    payload = ProcessSpec(
        id=step_id,
        needs=needs,
        capability=capability,
        adapter=_scaffold_adapter(package_id, step_id, adapter_kind),
    ).model_dump(mode="json", by_alias=True)
    if policy:
        payload.update(json.loads(json.dumps(policy)))
    payload["id"] = step_id
    payload["needs"] = needs
    payload["capability"] = capability
    return ProcessSpec.model_validate(payload)


def _scaffold_process_needs(
    *,
    step_id: str,
    index: int,
    steps: list[str],
    needs_by_step: dict[str, list[str]] | None,
) -> list[str]:
    if needs_by_step is not None and step_id in needs_by_step:
        return list(needs_by_step[step_id])
    return [steps[index - 1]] if index > 0 else []


def _parse_scaffold_steps(value: str) -> list[str]:
    steps = [part.strip() for part in value.split(",") if part.strip()]
    if not steps:
        raise ValueError("--steps must include at least one process id")
    if len(set(steps)) != len(steps):
        raise ValueError("--steps must not contain duplicate process ids")
    for step in steps:
        ProcessSpec(id=step, adapter={"kind": "queue", "queue": "validate.id"})
    return steps


def _write_new_file(path: Path, text: str) -> Path:
    if path.exists():
        raise ValueError(f"Refusing to overwrite existing file: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _package_yaml(package: WorkflowPackageSpec) -> str:
    text = (
        f"package: {package.id}\n"
        f"title: {_yaml_string(package.title or _title_from_id(package.id))}\n"
        f"version: {_yaml_string(package.version)}\n"
    )
    if package.document_types:
        text += "document_types:\n"
        for document_type in package.document_types:
            text += (
                f"  - id: {document_type.id}\n"
                f"    title: {_yaml_string(document_type.title or _title_from_id(document_type.id))}\n"
            )
            if document_type.media_types:
                text += f"    media_types: {_json_list(document_type.media_types)}\n"
            if document_type.extensions:
                text += f"    extensions: {_json_list(document_type.extensions)}\n"
            text += _yaml_mapping(
                "value_schema",
                document_type.value_schema,
                indent="    ",
            )
            text += _yaml_mapping(
                "metadata_schema",
                document_type.metadata_schema,
                indent="    ",
            )
    if package.document_relations:
        text += "document_relations:\n"
        for relation in package.document_relations:
            text += f"  - id: {relation.id}\n"
            if relation.title:
                text += f"    title: {_yaml_string(relation.title)}\n"
            if relation.description:
                text += (
                    f"    description: {_yaml_string(relation.description)}\n"
                )
            if relation.tags:
                text += f"    tags: {_json_list(relation.tags)}\n"
            if relation.source_document_types:
                text += (
                    "    source_document_types: "
                    f"{_json_list(relation.source_document_types)}\n"
                )
            if relation.target_document_types:
                text += (
                    "    target_document_types: "
                    f"{_json_list(relation.target_document_types)}\n"
                )
    if package.operation_types:
        text += "operation_types:\n"
        for operation in package.operation_types:
            text += f"  - id: {operation.id}\n"
            if operation.title:
                text += f"    title: {_yaml_string(operation.title)}\n"
            if operation.description:
                text += (
                    f"    description: {_yaml_string(operation.description)}\n"
                )
            if operation.tags:
                text += f"    tags: {_json_list(operation.tags)}\n"
            if operation.category:
                text += f"    category: {operation.category}\n"
    if package.artifact_kinds:
        text += "artifact_kinds:\n"
        for artifact_kind in package.artifact_kinds:
            text += (
                f"  - id: {artifact_kind.id}\n"
                f"    title: {_yaml_string(artifact_kind.title or _title_from_id(artifact_kind.id))}\n"
            )
            if artifact_kind.media_types:
                text += f"    media_types: {_json_list(artifact_kind.media_types)}\n"
            if artifact_kind.extensions:
                text += f"    extensions: {_json_list(artifact_kind.extensions)}\n"
            text += _yaml_mapping(
                "value_schema",
                artifact_kind.value_schema,
                indent="    ",
            )
            text += _yaml_mapping(
                "metadata_schema",
                artifact_kind.metadata_schema,
                indent="    ",
            )
    if package.capabilities:
        text += "capabilities:\n"
        for capability in package.capabilities:
            text += (
                f"  - id: {capability.id}\n"
                f"    title: {_yaml_string(capability.title or _title_from_id(capability.id))}\n"
            )
            if capability.operation_type:
                text += f"    operation_type: {capability.operation_type}\n"
            if capability.accepts_document_types:
                text += (
                    "    accepts_document_types: "
                    f"{_json_list(capability.accepts_document_types)}\n"
                )
            if capability.accepts_artifact_kinds:
                text += (
                    "    accepts_artifact_kinds: "
                    f"{_json_list(capability.accepts_artifact_kinds)}\n"
                )
            if capability.emits_document_types:
                text += (
                    "    emits_document_types: "
                    f"{_json_list(capability.emits_document_types)}\n"
                )
            if capability.emits_artifact_kinds:
                text += (
                    "    emits_artifact_kinds: "
                    f"{_json_list(capability.emits_artifact_kinds)}\n"
                )
            if capability.emits_streams:
                text += "    emits_streams:\n"
                for stream in capability.emits_streams:
                    text += _stream_spec_yaml(stream)
            text += _yaml_mapping(
                "config_schema",
                capability.config_schema,
                indent="    ",
            )
            text += _yaml_mapping(
                "output_schema",
                capability.output_schema,
                indent="    ",
            )
    text += "pipelines:\n" + "".join(
        f"  - {pipeline}\n" for pipeline in package.pipelines
    )
    if package.workers:
        text += "workers:\n"
        for worker in package.workers:
            text += (
                f"  - id: {worker.id}\n"
                f"    title: {_yaml_string(worker.title or _title_from_id(worker.id))}\n"
                f"    pipeline: {worker.pipeline_id}\n"
            )
            if worker.process_id:
                text += f"    process: {worker.process_id}\n"
            if worker.capabilities:
                text += f"    capabilities: {_json_list(worker.capabilities)}\n"
            text += (
                f"    adapter_kind: {worker.adapter_kind}\n"
                f"    command: {_json_list(worker.command)}\n"
            )
            if worker.cwd is not None:
                text += f"    cwd: {_yaml_string(worker.cwd)}\n"
            if worker.env:
                text += "    env:\n"
                for key, value in sorted(worker.env.items()):
                    text += f"      {key}: {_yaml_string(value)}\n"
            if worker.timeout_seconds is not None:
                text += f"    timeout_seconds: {worker.timeout_seconds}\n"
    return text


def _pipeline_yaml(pipeline: PipelineSpec) -> str:
    lines = [
        f"pipeline: {pipeline.id}",
        f"title: {_yaml_string(pipeline.title or _title_from_id(pipeline.id))}",
        "steps:",
    ]
    for step in pipeline.steps:
        lines.extend(
            [
                f"  - id: {step.id}",
            ]
        )
        if step.needs:
            lines.append(
                "    needs: ["
                + ", ".join(_yaml_string(need) for need in step.needs)
                + "]"
            )
        if step.capability:
            lines.append(f"    capability: {step.capability}")
        if step.title:
            lines.append(f"    title: {_yaml_string(step.title)}")
        if step.description:
            lines.append(f"    description: {_yaml_string(step.description)}")
        if step.tags:
            lines.append(
                "    tags: ["
                + ", ".join(_yaml_string(tag) for tag in step.tags)
                + "]"
            )
        if step.timeout_seconds is not None:
            lines.append(f"    timeout_seconds: {step.timeout_seconds}")
        if step.priority:
            lines.append(f"    priority: {step.priority}")
        if step.max_concurrency is not None:
            lines.append(f"    max_concurrency: {step.max_concurrency}")
        if step.resource_pool != "default":
            lines.append(f"    resource_pool: {step.resource_pool}")
        retry = step.retry.model_dump(
            mode="json",
            exclude_defaults=True,
            exclude_none=True,
        )
        if retry:
            lines.extend(_yaml_block("retry", retry, indent="    "))
        sla = step.sla.model_dump(
            mode="json",
            exclude_defaults=True,
            exclude_none=True,
        )
        if sla:
            lines.extend(_yaml_block("sla", sla, indent="    "))
        resources = step.resources.model_dump(
            mode="json",
            exclude_defaults=True,
            exclude_none=True,
        )
        if resources:
            lines.extend(_yaml_block("resources", resources, indent="    "))
        when = step.when.model_dump(
            mode="json",
            exclude_defaults=True,
            exclude_none=True,
        )
        if when:
            lines.extend(_yaml_block("when", when, indent="    "))
        if step.wait_for_children is not None:
            lines.extend(
                _yaml_block(
                    "wait_for_children",
                    step.wait_for_children.model_dump(
                        mode="json",
                        exclude_defaults=True,
                        exclude_none=True,
                    ),
                    indent="    ",
                )
            )
        if step.config:
            lines.extend(_yaml_block("config", step.config, indent="    "))
        lines.extend(_adapter_yaml(step.adapter, indent="    "))
    lines.extend(
        [
            "combines:",
            "  - id: workflow_result",
            "    mode: latest",
            "    emit_partial: true",
            "    needs: ["
            + ", ".join(_yaml_string(step.id) for step in pipeline.steps)
            + "]",
        ]
    )
    return "\n".join(lines) + "\n"


def _scaffold_step_guidance(
    *,
    step_id: str,
    document_type: str,
    capability: str,
    operation_type: str,
    artifact_kind: str,
    needs: list[str],
    accepts_document_types: list[str],
    accepts_artifact_kinds: list[str],
    emits_document_types: list[str],
    streams: list[StreamSpec],
    guidance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    stream_ids = [stream.stream_id for stream in streams]
    result: dict[str, Any] = {
        "role": _title_from_id(step_id),
        "operation_type": operation_type,
        "capability": capability,
        "artifact_kind": artifact_kind,
        "intent": (
            f"Implement {operation_type} work for capability {capability} "
            f"in a {document_type} workflow."
        ),
        "inputs": {
            "needs": list(needs),
            "document_types": list(accepts_document_types),
            "artifact_kinds": list(accepts_artifact_kinds),
        },
        "outputs": {
            "artifact_kind": artifact_kind,
            "document_types": list(emits_document_types),
            "streams": stream_ids,
        },
        "replace_sample_with": [
            "Read source documents and upstream artifacts from ProcessExecutionContext.",
            "Write durable artifacts under artifact_root(context, PROCESS_ID).",
            "Return ProcessOutput values that match the capability output schema.",
        ],
    }
    if guidance:
        result.update(guidance)
    return result


def _step_program_source(
    *,
    step_id: str,
    artifact_kind: str,
    artifact_value_schema: dict[str, Any],
    output_schema: dict[str, Any],
    streams: list[StreamSpec] | None = None,
    guidance: dict[str, Any] | None = None,
) -> str:
    stream_specs = [
        stream.model_dump(mode="json", by_alias=True)
        for stream in streams or []
    ]
    stream_literal = pprint.pformat(stream_specs, sort_dicts=False, width=88)
    artifact_schema_literal = pprint.pformat(
        artifact_value_schema,
        sort_dicts=False,
        width=88,
    )
    output_schema_literal = pprint.pformat(
        output_schema,
        sort_dicts=False,
        width=88,
    )
    guidance_literal = pprint.pformat(
        guidance or {},
        sort_dicts=False,
        width=88,
    )
    return f'''from __future__ import annotations

from fala.sdk import artifact, artifact_root, emit_event, output, output_document, run_stdio, stream_chunk, write_json

PROCESS_ID = {step_id!r}
OUTPUT_KIND = {artifact_kind!r}
ARTIFACT_VALUE_SCHEMA = {artifact_schema_literal}
OUTPUT_SCHEMA = {output_schema_literal}
STREAMS = {stream_literal}
WORKER_GUIDANCE = {guidance_literal}


def run(context):
    emit_event(
        "process.progress",
        status="running",
        data={{
            "process_id": PROCESS_ID,
            "stage": "started",
            "worker_guidance": WORKER_GUIDANCE,
        }},
    )
    result = _sample_values(OUTPUT_SCHEMA, context)
    artifact_payload = _sample_values(ARTIFACT_VALUE_SCHEMA, context)
    path = write_json(
        artifact_root(context, PROCESS_ID) / f"{{PROCESS_ID}}.json",
        artifact_payload,
    )
    return output(
        values=result,
        artifacts=[artifact(OUTPUT_KIND, path)],
        metadata={{
            "capability": context.get("capability"),
            "worker_guidance": WORKER_GUIDANCE,
        }},
        stream_chunks=[
            _sample_stream_chunk(stream, context)
            for stream in STREAMS
        ],
    )


def _sample_stream_chunk(stream, context):
    return stream_chunk(
        stream_id=stream.get("stream") or stream.get("stream_id") or "main",
        kind=(stream.get("kinds") or [None])[0],
        values=_sample_values(stream.get("value_schema") or {{}}, context),
        metadata=_sample_values(stream.get("metadata_schema") or {{}}, context),
    )


def _sample_values(schema, context):
    properties = schema.get("properties") or {{}}
    required = list(schema.get("required") or [])
    values = {{}}
    for key in required:
        values[key] = _sample_value_for_key(key, properties.get(key) or {{}}, context)
    for key in ("page_number", "chunk_index", "dimension", "score", "frame"):
        if key in properties and key not in values:
            values[key] = _sample_value_for_key(key, properties[key], context)
    return values


def _sample_value_for_key(key, schema, context):
    if "const" in schema:
        return schema["const"]
    if key == "text":
        return f"{{PROCESS_ID}} sample text"
    if key == "process_id":
        return PROCESS_ID
    if key in {{"asset_id", "embedding_id"}}:
        return f"{{PROCESS_ID}}-{{context.get('document_id') or 'document'}}"
    if key == "status":
        return "ok"
    if key == "filename":
        return f"{{PROCESS_ID}}.bin"
    if key == "document_id":
        return context.get("document_id") or "document"
    if key in {{"page_number", "dimension"}}:
        return 1
    if key in {{"chunk_index", "frame"}}:
        return 0
    if key == "score":
        return 1.0
    kind = schema.get("type")
    if isinstance(kind, list):
        kind = next((item for item in kind if item != "null"), "string")
    if kind == "integer":
        return 0
    if kind == "number":
        return 0.0
    if kind == "boolean":
        return True
    if kind == "array":
        return []
    if kind == "object":
        return {{}}
    return "sample"


if __name__ == "__main__":
    raise SystemExit(run_stdio(run))
'''


def _title_from_id(value: str) -> str:
    return value.replace("_", " ").replace("-", " ").strip().title() or value


def _yaml_string(value: str) -> str:
    return json.dumps(value)


def _json_list(values: list[str]) -> str:
    return "[" + ", ".join(_yaml_string(value) for value in values) + "]"


def _yaml_mapping(key: str, value: dict[str, Any], *, indent: str) -> str:
    if not value:
        return ""
    lines = yaml.safe_dump(
        value,
        default_flow_style=False,
        sort_keys=False,
    ).splitlines()
    return f"{indent}{key}:\n" + "".join(
        f"{indent}  {line}\n" for line in lines
    )


def _yaml_block(key: str, value: dict[str, Any], *, indent: str) -> list[str]:
    if not value:
        return []
    text = _yaml_mapping(key, value, indent=indent)
    return text.rstrip("\n").splitlines()


def _adapter_yaml(adapter: Any, *, indent: str) -> list[str]:
    lines = [
        f"{indent}adapter:",
        f"{indent}  kind: {adapter.kind}",
    ]
    if adapter.command:
        lines.append(f"{indent}  command: {_json_list(adapter.command)}")
    if adapter.cwd:
        lines.append(f"{indent}  cwd: {_yaml_string(adapter.cwd)}")
    if adapter.env:
        lines.extend(_yaml_block("env", adapter.env, indent=f"{indent}  "))
    if adapter.url:
        lines.append(f"{indent}  url: {_yaml_string(adapter.url)}")
    if adapter.queue:
        lines.append(f"{indent}  queue: {_yaml_string(adapter.queue)}")
    if adapter.timeout_seconds is not None:
        lines.append(f"{indent}  timeout_seconds: {adapter.timeout_seconds}")
    return lines


def _stream_spec_yaml(stream: StreamSpec) -> str:
    text = f"      - stream: {stream.stream_id}\n"
    if stream.kinds:
        text += f"        kinds: {_json_list(stream.kinds)}\n"
    if stream.consumers:
        text += f"        consumers: {_json_list(stream.consumers)}\n"
    if stream.emits_artifact_kinds:
        text += (
            "        emits_artifact_kinds: "
            f"{_json_list(stream.emits_artifact_kinds)}\n"
        )
    if stream.max_buffered_chunks is not None:
        text += f"        max_buffered_chunks: {stream.max_buffered_chunks}\n"
    text += _yaml_mapping("value_schema", stream.value_schema, indent="        ")
    text += _yaml_mapping("metadata_schema", stream.metadata_schema, indent="        ")
    return text


def _step_by_id(pipeline: PipelineSpec, process_id: str) -> ProcessSpec:
    for step in pipeline.steps:
        if step.id == process_id:
            return step
    raise ValueError(f"Unknown process id {process_id!r} for pipeline {pipeline.id!r}")


async def _refresh_projections_for_process(
    *,
    store: StateStore,
    pipeline: PipelineSpec,
    run_id: str,
    document_id: str,
    process_id: str,
) -> list[CombinedProjection]:
    refreshed: list[CombinedProjection] = []
    for combine in pipeline.combines:
        if process_id not in combine.needs:
            continue

        latest = await store.get_outputs(
            run_id=run_id,
            document_id=document_id,
            process_ids=combine.needs,
        )
        complete = set(latest) == set(combine.needs)
        if not complete and not combine.emit_partial:
            continue

        projection = CombinedProjection(
            id=combine.id,
            run_id=run_id,
            document_id=document_id,
            complete=complete,
            latest=latest,
        )
        await store.put_projection(projection)
        await store.append_event(
            ProcessEvent(
                run_id=run_id,
                document_id=document_id,
                process_id=None,
                type="projection.updated",
                data={
                    "projection_id": combine.id,
                    "complete": complete,
                    "process_ids": sorted(latest),
                },
            )
        )
        refreshed.append(projection)
    return refreshed


def _print_plain_validation(payload: dict[str, Any]) -> None:
    print(
        f"ok: {payload['pipeline_count']} pipeline(s), "
        f"{payload.get('package_count', 0)} package(s)"
    )
    for pipeline in payload["pipelines"]:
        print(
            f"- {pipeline['id']} v{pipeline['version']}: "
            f"{len(pipeline['steps'])} step(s), {len(pipeline['combines'])} combine(s)"
        )
    return None


def _inspect_runtime_run_input(source: str) -> dict[str, Any]:
    data = _read_runtime_run_input_mapping(source)
    raw_documents = data.get("documents") or []
    if not isinstance(raw_documents, list):
        raise ValueError("RuntimeRunInput documents must be a list")

    issues: list[dict[str, Any]] = []
    document_rows: list[dict[str, Any]] = []
    document_rows_by_index: dict[int, dict[str, Any]] = {}
    duplicate_document_ids: dict[str, list[int]] = defaultdict(list)
    duplicate_source_sha256: dict[str, list[int]] = defaultdict(list)
    duplicate_source_uri: dict[str, list[int]] = defaultdict(list)

    for index, raw_document in enumerate(raw_documents):
        if not isinstance(raw_document, dict):
            issues.append(
                {
                    "severity": "error",
                    "type": "invalid_document",
                    "index": index,
                    "message": "document entry must be an object",
                }
            )
            continue
        document = _inspect_document_row(raw_document, index=index)
        document_rows.append(document)
        document_rows_by_index[index] = document
        if document["document_id"]:
            duplicate_document_ids[document["document_id"]].append(index)
        if document["source_sha256"]:
            duplicate_source_sha256[document["source_sha256"]].append(index)
        if document["source_uri"]:
            duplicate_source_uri[document["source_uri"]].append(index)

    for document_id, indexes in sorted(duplicate_document_ids.items()):
        if len(indexes) > 1:
            issues.append(
                {
                    "severity": "error",
                    "type": "duplicate_document_id",
                    "document_id": document_id,
                    "count": len(indexes),
                    "indexes": indexes,
                }
            )
    for source_sha256, indexes in sorted(duplicate_source_sha256.items()):
        if len(indexes) > 1:
            issues.append(
                {
                    "severity": "warning",
                    "type": "duplicate_source_sha256",
                    "source_sha256": source_sha256,
                    "count": len(indexes),
                    "indexes": indexes,
                    "document_ids": [
                        document_rows_by_index[index]["document_id"]
                        for index in indexes
                        if index in document_rows_by_index
                    ],
                }
            )
    for source_uri, indexes in sorted(duplicate_source_uri.items()):
        if len(indexes) > 1:
            issues.append(
                {
                    "severity": "warning",
                    "type": "duplicate_source_uri",
                    "source_uri": source_uri,
                    "count": len(indexes),
                    "indexes": indexes,
                    "document_ids": [
                        document_rows_by_index[index]["document_id"]
                        for index in indexes
                        if index in document_rows_by_index
                    ],
                }
            )

    summary = _inspect_document_summary(
        document_rows=document_rows,
        pipeline_id=data.get("pipeline_id"),
    )
    error_count = sum(1 for issue in issues if issue["severity"] == "error")
    warning_count = sum(1 for issue in issues if issue["severity"] == "warning")
    return {
        "ok": error_count == 0,
        "run_id": data.get("run_id"),
        "pipeline_id": data.get("pipeline_id"),
        "document_count": len(raw_documents),
        "document_summary": summary,
        "issue_count": len(issues),
        "error_count": error_count,
        "warning_count": warning_count,
        "issues": issues,
    }


def _inspect_document_row(raw_document: dict[str, Any], *, index: int) -> dict[str, Any]:
    metadata = raw_document.get("metadata") if isinstance(raw_document.get("metadata"), dict) else {}
    values = raw_document.get("values") if isinstance(raw_document.get("values"), dict) else {}
    artifacts = raw_document.get("artifacts") if isinstance(raw_document.get("artifacts"), list) else []
    source_sha256 = (
        str(metadata.get("source_sha256") or raw_document.get("source_sha256") or "")
        .removeprefix("sha256:")
        .strip()
    )
    return {
        "index": index,
        "document_id": str(raw_document.get("document_id") or "").strip(),
        "pipeline_id": str(raw_document.get("pipeline_id") or "").strip(),
        "document_type": str(raw_document.get("document_type") or "").strip(),
        "media_type": str(raw_document.get("media_type") or "").strip(),
        "source_uri": str(raw_document.get("source_uri") or "").strip(),
        "source_sha256": source_sha256,
        "value_keys": sorted(str(key) for key in values),
        "metadata_keys": sorted(str(key) for key in metadata),
        "artifact_kinds": sorted(
            str(artifact.get("kind"))
            for artifact in artifacts
            if isinstance(artifact, dict) and artifact.get("kind")
        ),
    }


def _inspect_document_summary(
    *,
    document_rows: list[dict[str, Any]],
    pipeline_id: Any,
) -> dict[str, Any]:
    pipeline_counts: Counter[str] = Counter()
    document_type_counts: Counter[str] = Counter()
    media_type_counts: Counter[str] = Counter()
    source_scheme_counts: Counter[str] = Counter()
    artifact_kind_counts: Counter[str] = Counter()
    value_keys: set[str] = set()
    metadata_keys: set[str] = set()
    missing_document_id_count = 0
    missing_document_type_count = 0
    missing_media_type_count = 0
    with_source_uri_count = 0
    with_source_sha256_count = 0

    for document in document_rows:
        pipeline_counts[document["pipeline_id"] or str(pipeline_id or "<missing>")] += 1
        if not document["document_id"]:
            missing_document_id_count += 1
        if document["document_type"]:
            document_type_counts[document["document_type"]] += 1
        else:
            missing_document_type_count += 1
        if document["media_type"]:
            media_type_counts[document["media_type"]] += 1
        else:
            missing_media_type_count += 1
        if document["source_uri"]:
            with_source_uri_count += 1
            parsed = urlparse(document["source_uri"])
            source_scheme_counts[parsed.scheme or "<none>"] += 1
        else:
            source_scheme_counts["<missing>"] += 1
        if document["source_sha256"]:
            with_source_sha256_count += 1
        value_keys.update(document["value_keys"])
        metadata_keys.update(document["metadata_keys"])
        artifact_kind_counts.update(document["artifact_kinds"])

    return {
        "document_count": len(document_rows),
        "pipeline_counts": _sorted_counter(pipeline_counts),
        "document_type_counts": _sorted_counter(document_type_counts),
        "media_type_counts": _sorted_counter(media_type_counts),
        "source_scheme_counts": _sorted_counter(source_scheme_counts),
        "artifact_kind_counts": _sorted_counter(artifact_kind_counts),
        "value_keys": sorted(value_keys),
        "metadata_keys": sorted(metadata_keys),
        "with_source_uri_count": with_source_uri_count,
        "with_source_sha256_count": with_source_sha256_count,
        "missing_document_id_count": missing_document_id_count,
        "missing_document_type_count": missing_document_type_count,
        "missing_media_type_count": missing_media_type_count,
    }


def _sorted_counter(counter: Counter[str]) -> dict[str, int]:
    return {key: counter[key] for key in sorted(counter)}


def _discover_runtime_run_input(
    args: argparse.Namespace,
    *,
    registry: PipelineRegistry | None = None,
) -> RuntimeRunInput:
    run_input, _route_report = _discover_runtime_run_input_with_report(
        args,
        registry=registry,
    )
    return run_input


def _discover_runtime_run_input_with_report(
    args: argparse.Namespace,
    *,
    registry: PipelineRegistry | None = None,
) -> tuple[RuntimeRunInput, dict[str, Any]]:
    routes = _read_document_routes(args.route)
    auto_routes = (
        auto_document_routes_from_registry(registry)
        if args.auto_route and registry is not None
        else []
    )
    documents = _discover_runtime_document_inputs(
        input_dirs=args.input_dir,
        files=args.file,
        source_lists=args.source_list,
        include_patterns=args.include or ["*"],
        exclude_patterns=args.exclude,
        recursive=not args.no_recursive,
        content_hash=args.content_hash,
        document_id_mode=args.document_id_mode,
        document_type=args.document_type,
        media_type=args.media_type,
        values=_parse_values(args.value),
        metadata=_parse_values(args.metadata),
    )
    documents, route_report = route_runtime_documents_with_report(
        documents,
        routes=routes,
        auto_routes=auto_routes,
    )
    if not documents:
        raise ValueError("discover-documents found no matching files")
    return (
        RuntimeRunInput(
            run_id=args.run_id,
            existing_run_policy=args.existing_run,
            existing_document_policy=args.existing_document,
            title=args.title,
            pipeline_id=args.pipeline,
            documents=documents,
        ),
        route_report,
    )


def _discover_runtime_document_inputs(
    *,
    input_dirs: list[str],
    files: list[str],
    source_lists: list[str],
    include_patterns: list[str],
    exclude_patterns: list[str],
    recursive: bool,
    content_hash: bool,
    document_id_mode: str,
    document_type: str | None,
    media_type: str | None,
    values: dict[str, Any],
    metadata: dict[str, Any],
) -> list[RuntimeDocumentInput]:
    if not input_dirs and not files and not source_lists:
        raise ValueError("discover-documents requires --input-dir, --file, or --source-list")
    discovered: list[RuntimeDocumentInput] = []
    for item in source_lists:
        discovered.extend(
            _runtime_document_inputs_from_source_list(
                source=Path(item).expanduser(),
                include_patterns=include_patterns,
                exclude_patterns=exclude_patterns,
                content_hash=content_hash,
                document_id_mode=document_id_mode,
                document_type=document_type,
                media_type=media_type,
                values=values,
                metadata=metadata,
            )
        )

    for item in input_dirs:
        root = Path(item).expanduser().resolve()
        if not root.exists() or not root.is_dir():
            raise ValueError(f"Input directory does not exist: {root}")
        iterator = root.rglob("*") if recursive else root.glob("*")
        for path in sorted(item.resolve() for item in iterator if item.is_file()):
            relative_path = path.relative_to(root).as_posix()
            source_sha256 = _sha256_file(path) if content_hash or document_id_mode == "sha256" else None
            document_id = _document_id_from_path(
                path=path,
                relative_path=relative_path,
                mode=document_id_mode,
                source_sha256=source_sha256,
            )
            if not _path_matches(document_id, include_patterns, exclude_patterns):
                continue
            discovered.append(
                _runtime_document_input_from_path(
                    path=path,
                    document_id=document_id,
                    source_sha256=source_sha256,
                    document_type=document_type,
                    media_type=media_type,
                    values=values,
                    metadata=metadata,
                )
            )

    for item in files:
        path = Path(item).expanduser().resolve()
        if not path.exists() or not path.is_file():
            raise ValueError(f"Document file does not exist: {path}")
        source_sha256 = _sha256_file(path) if content_hash or document_id_mode == "sha256" else None
        document_id = _document_id_from_path(
            path=path,
            relative_path=path.name,
            mode=document_id_mode,
            source_sha256=source_sha256,
        )
        if not _path_matches(document_id, include_patterns, exclude_patterns):
            continue
        discovered.append(
            _runtime_document_input_from_path(
                path=path,
                document_id=document_id,
                source_sha256=source_sha256,
                document_type=document_type,
                media_type=media_type,
                values=values,
                metadata=metadata,
            )
        )
    return discovered


def _runtime_document_input_from_path(
    *,
    path: Path,
    document_id: str,
    source_sha256: str | None,
    document_type: str | None,
    media_type: str | None,
    values: dict[str, Any],
    metadata: dict[str, Any],
) -> RuntimeDocumentInput:
    stat = path.stat()
    guessed_media_type = media_type or mimetypes.guess_type(path.name)[0]
    source_metadata: dict[str, Any] = {
        **metadata,
        "source_path": str(path),
        "source_size": stat.st_size,
        "source_mtime": stat.st_mtime,
    }
    if source_sha256 is not None:
        source_metadata["source_sha256"] = source_sha256
    return RuntimeDocumentInput(
        document_id=document_id,
        title=path.name,
        document_type=document_type,
        media_type=guessed_media_type or "application/octet-stream",
        source_uri=path.as_uri(),
        values=dict(values),
        metadata=source_metadata,
    )


def _runtime_document_inputs_from_source_list(
    *,
    source: Path,
    include_patterns: list[str],
    exclude_patterns: list[str],
    content_hash: bool,
    document_id_mode: str,
    document_type: str | None,
    media_type: str | None,
    values: dict[str, Any],
    metadata: dict[str, Any],
) -> list[RuntimeDocumentInput]:
    path = source.resolve()
    if not path.exists() or not path.is_file():
        raise ValueError(f"Source list does not exist: {path}")
    delimiter = "\t" if path.suffix.lower() == ".tsv" else ","
    rows: list[RuntimeDocumentInput] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=delimiter)
        if not reader.fieldnames:
            raise ValueError(f"Source list has no header row: {path}")
        for row_number, row in enumerate(reader, start=2):
            row = {str(key or "").strip(): (value or "") for key, value in row.items()}
            document = _runtime_document_input_from_source_row(
                row=row,
                row_number=row_number,
                source_list=path,
                content_hash=content_hash,
                document_id_mode=document_id_mode,
                document_type=document_type,
                media_type=media_type,
                values=values,
                metadata=metadata,
            )
            if not _path_matches(document.document_id, include_patterns, exclude_patterns):
                continue
            rows.append(document)
    return rows


def _runtime_document_input_from_source_row(
    *,
    row: dict[str, str],
    row_number: int,
    source_list: Path,
    content_hash: bool,
    document_id_mode: str,
    document_type: str | None,
    media_type: str | None,
    values: dict[str, Any],
    metadata: dict[str, Any],
) -> RuntimeDocumentInput:
    source_uri = row.get("source_uri", "").strip()
    source_path = (row.get("path") or row.get("source_path") or "").strip()
    local_path: Path | None = None
    if not source_uri and source_path:
        local_path = Path(source_path).expanduser()
        if not local_path.is_absolute():
            local_path = source_list.parent / local_path
        local_path = local_path.resolve()
        source_uri = local_path.as_uri()
    if not source_uri:
        raise ValueError(
            f"Source list row {row_number} requires source_uri, path, or source_path"
        )

    source_sha256 = _source_sha256_from_row(row)
    if (
        source_sha256 is None
        and local_path is not None
        and local_path.exists()
        and local_path.is_file()
        and (content_hash or document_id_mode == "sha256")
    ):
        source_sha256 = _sha256_file(local_path)
    document_id = (row.get("document_id") or "").strip() or _document_id_from_source(
        source_uri=source_uri,
        fallback=f"row_{row_number}",
        mode=document_id_mode,
        source_sha256=source_sha256,
    )
    title = (row.get("title") or "").strip() or document_id
    row_values = {
        key.removeprefix("value."): _parse_source_list_cell(value)
        for key, value in row.items()
        if key.startswith("value.") and value != ""
    }
    row_metadata = {
        key.removeprefix("metadata."): _parse_source_list_cell(value)
        for key, value in row.items()
        if key.startswith("metadata.") and value != ""
    }
    resolved_media_type = (
        (row.get("media_type") or "").strip()
        or media_type
        or _guess_media_type_from_source_uri(source_uri)
        or "application/octet-stream"
    )
    resolved_document_type = (row.get("document_type") or "").strip() or document_type
    source_metadata = {
        **metadata,
        **row_metadata,
        "source_list": str(source_list),
        "source_list_row": row_number,
    }
    if source_sha256 is not None:
        source_metadata["source_sha256"] = source_sha256
    return RuntimeDocumentInput(
        document_id=document_id,
        pipeline_id=(row.get("pipeline_id") or row.get("pipeline") or "").strip()
        or None,
        title=title,
        document_type=resolved_document_type,
        relation=(row.get("relation") or "").strip() or None,
        parent_document_id=(row.get("parent_document_id") or "").strip()
        or None,
        parent_process_id=(row.get("parent_process_id") or "").strip()
        or None,
        media_type=resolved_media_type,
        source_uri=source_uri,
        values={**values, **row_values},
        metadata=source_metadata,
    )


def _parse_source_list_cell(value: str) -> Any:
    text = value.strip()
    if not text:
        return ""
    try:
        parsed = yaml.safe_load(text)
    except yaml.YAMLError:
        return value
    return text if parsed is None and text.lower() not in {"null", "~"} else parsed


def _read_document_routes(paths: list[str]) -> list[dict[str, Any]]:
    routes: list[dict[str, Any]] = []
    for raw_path in paths:
        data = _read_yaml_value(raw_path, label="document route")
        routes.extend(
            coerce_document_routes(data, source=f"Document route {raw_path!r}")
        )
    return routes


def _document_id_from_path(
    *,
    path: Path,
    relative_path: str,
    mode: str,
    source_sha256: str | None,
) -> str:
    if mode == "path":
        return relative_path
    if mode == "name":
        return path.name
    if mode == "sha256":
        if source_sha256 is None:
            raise ValueError(f"Cannot use sha256 document id without content hash: {path}")
        return f"sha256:{source_sha256}"
    raise ValueError(f"Unsupported document id mode: {mode}")


def _document_id_from_source(
    *,
    source_uri: str,
    fallback: str,
    mode: str,
    source_sha256: str | None,
) -> str:
    if mode == "sha256":
        if source_sha256 is None:
            raise ValueError(
                f"Cannot use sha256 document id without source_sha256: {source_uri}"
            )
        return f"sha256:{source_sha256}"
    if mode == "name":
        name = _source_uri_name(source_uri)
        return name or fallback
    if mode == "path":
        return _source_uri_path(source_uri) or fallback
    raise ValueError(f"Unsupported document id mode: {mode}")


def _source_uri_name(source_uri: str) -> str:
    parsed = urlparse(source_uri)
    return Path(unquote(parsed.path)).name


def _source_uri_path(source_uri: str) -> str:
    parsed = urlparse(source_uri)
    path = unquote(parsed.path).strip("/")
    return path or _source_uri_name(source_uri)


def _guess_media_type_from_source_uri(source_uri: str) -> str | None:
    parsed = urlparse(source_uri)
    name = Path(unquote(parsed.path)).name
    return mimetypes.guess_type(name)[0] if name else None


def _source_sha256_from_row(row: dict[str, str]) -> str | None:
    for key in ("source_sha256", "sha256", "source_hash"):
        value = (row.get(key) or "").strip()
        if value:
            return value.removeprefix("sha256:")
    return None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _path_matches(
    document_id: str,
    include_patterns: list[str],
    exclude_patterns: list[str],
) -> bool:
    return any(fnmatch.fnmatch(document_id, pattern) for pattern in include_patterns) and not any(
        fnmatch.fnmatch(document_id, pattern) for pattern in exclude_patterns
    )


def _runtime_run_input_from_args(
    args: argparse.Namespace,
    *,
    command_label: str,
) -> RuntimeRunInput:
    base = _load_runtime_run_input(args.run_input) if args.run_input else RuntimeRunInput()
    data = base.model_dump(mode="python")

    if args.run_id is not None:
        data["run_id"] = args.run_id
    if args.existing_run is not None:
        data["existing_run_policy"] = args.existing_run
    if args.existing_document is not None:
        data["existing_document_policy"] = args.existing_document
    if args.title is not None:
        data["title"] = args.title
    if args.pipeline is not None:
        data["pipeline_id"] = args.pipeline

    data["config"] = _merge_run_config(data.get("config") or {}, _parse_run_config(args))
    data["documents"] = [
        *data.get("documents", []),
        *[
            document.model_dump(mode="python")
            for document in _parse_runtime_document_inputs(
                files=args.file,
                documents=args.document,
                document_type=args.document_type,
                media_type=args.media_type,
                values=_parse_values(args.value),
                metadata=_parse_values(args.metadata),
                require_any=False,
            )
        ],
    ]
    if not data["documents"]:
        raise ValueError(
            f"{command_label} requires documents from --run-input, --file, or --document"
        )
    return RuntimeRunInput.model_validate(data)


def _load_runtime_run_input(source: str) -> RuntimeRunInput:
    data = _read_runtime_run_input_mapping(source)
    try:
        return RuntimeRunInput.model_validate(data)
    except Exception as exc:
        raise ValueError(f"RuntimeRunInput manifest validation failed: {exc}") from exc


def _read_runtime_run_input_mapping(source: str) -> dict[str, Any]:
    text = sys.stdin.read() if source == "-" else Path(source).read_text(encoding="utf-8")
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid RuntimeRunInput manifest {source!r}: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"RuntimeRunInput manifest must contain an object: {source}")
    return data


def _merge_run_config(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if (
            key == "resource_pools"
            and isinstance(value, dict)
            and isinstance(merged.get(key), dict)
        ):
            pools = dict(merged[key])
            for pool_id, pool in value.items():
                merged_pool = {**pools.get(pool_id, {}), **pool}
                if (
                    isinstance(pools.get(pool_id, {}).get("units"), dict)
                    and isinstance(pool.get("units"), dict)
                ):
                    merged_pool["units"] = {
                        **pools[pool_id]["units"],
                        **pool["units"],
                    }
                pools[pool_id] = merged_pool
            merged[key] = pools
        else:
            merged[key] = value
    return merged


def _parse_values(items: list[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    for item in items:
        key, sep, value = item.partition("=")
        if not sep or not key:
            raise ValueError(f"Invalid --value {item!r}; expected key=value")
        values[key] = value
    return values


def _parse_artifacts(items: list[str]) -> list[ArtifactRef]:
    artifacts: list[ArtifactRef] = []
    for item in items:
        kind, sep, uri = item.partition("=")
        if not sep or not kind or not uri:
            raise ValueError(f"Invalid --artifact {item!r}; expected kind=uri")
        artifacts.append(ArtifactRef(kind=kind, uri=uri))
    return artifacts


def _parse_runtime_document_inputs(
    *,
    files: list[str],
    documents: list[str],
    document_type: str | None,
    media_type: str | None,
    values: dict[str, Any],
    metadata: dict[str, Any],
    command_label: str = "create-run",
    require_any: bool = True,
) -> list[RuntimeDocumentInput]:
    parsed: list[RuntimeDocumentInput] = []
    for item in files:
        path = Path(item).expanduser().resolve()
        if not path.exists() or not path.is_file():
            raise ValueError(f"Document file does not exist: {path}")
        parsed.append(
            RuntimeDocumentInput(
                document_id=path.name,
                title=path.name,
                document_type=document_type,
                media_type=media_type,
                source_uri=path.as_uri(),
                values=dict(values),
                metadata={**metadata, "source_path": str(path)},
            )
        )
    for item in documents:
        document_id, sep, uri = item.partition("=")
        if not sep or not document_id or not uri:
            raise ValueError(f"Invalid --document {item!r}; expected document_id=uri")
        parsed.append(
            RuntimeDocumentInput(
                document_id=document_id,
                title=document_id,
                document_type=document_type,
                media_type=media_type,
                source_uri=uri,
                values=dict(values),
                metadata=dict(metadata),
            )
        )
    if require_any and not parsed:
        raise ValueError(f"{command_label} requires at least one --file or --document")
    return parsed


if __name__ == "__main__":
    raise SystemExit(main())
