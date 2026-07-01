from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from html import escape as html_escape
from importlib import import_module
import json
import sqlite3
import sys
import time
import zipfile
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from pydantic import BaseModel

from fala.adapters import StepRunRequest, StepRunResult, create_step_adapter
from fala.artifacts import FileArtifactStore, digest_from_fala_artifact_uri
from fala.models import (
    ArtifactKindSpec,
    ArtifactRef,
    CarrierAdapterSpec,
    CarrierArtifactStoreConfig,
    CarrierCapabilitySpec,
    CarrierFlowSpec,
    CarrierFlowStepSpec,
    CarrierRelationSpec,
    CarrierRuntimeBackendConfig,
    CarrierRuntimeConfigSpec,
    CarrierTypeSpec,
    CarrierWorkflowPackageSpec,
    ObservationKindSpec,
)
from fala.runtime_backend import Artifact as CarrierArtifact
from fala.runtime_backend import BridgeDelivery
from fala.runtime_backend import BridgeDeliveryStatus
from fala.runtime_backend import Carrier
from fala.runtime_backend import CarrierProcessStatus
from fala.runtime_backend import CarrierRelation
from fala.runtime_backend import CarrierRunStatus
from fala.runtime_backend import CarrierType
from fala.runtime_backend import CommandSubmission
from fala.runtime_backend import DelegationPolicy
from fala.runtime_backend import EventRef
from fala.runtime_backend import Gate as CarrierGate
from fala.runtime_backend import GateStatus as CarrierGateStatus
from fala.runtime_backend import Observation
from fala.runtime_backend import Process as CarrierProcess
from fala.runtime_backend import Projection
from fala.runtime_backend import Run as CarrierRun
from fala.runtime_backend import RunRef
from fala.runtime_backend import RuntimeBackendService
from fala.runtime_backend import RuntimeBudget
from fala.runtime_backend import RuntimeCommand
from fala.runtime_backend import RuntimeEvent
from fala.runtime_backend import RuntimePool
from fala.runtime_backend import RuntimeRef
from fala.runtime_backend import SQLITE_RUNTIME_SCHEMA_VERSION
from fala.runtime_backend import SQLiteRuntimeBackend
from fala.yaml_loader import load_carrier_workflow_package_yaml

CONTRACT_MODELS: dict[str, type[BaseModel]] = {
    "adapter": CarrierAdapterSpec,
    "artifact": CarrierArtifact,
    "artifact-kind": ArtifactKindSpec,
    "artifact-ref": ArtifactRef,
    "carrier": Carrier,
    "carrier-artifact-store-config": CarrierArtifactStoreConfig,
    "carrier-capability": CarrierCapabilitySpec,
    "carrier-package": CarrierWorkflowPackageSpec,
    "carrier-flow": CarrierFlowSpec,
    "carrier-flow-step": CarrierFlowStepSpec,
    "carrier-relation": CarrierRelation,
    "carrier-relation-spec": CarrierRelationSpec,
    "carrier-runtime-backend-config": CarrierRuntimeBackendConfig,
    "carrier-runtime-config": CarrierRuntimeConfigSpec,
    "carrier-type": CarrierType,
    "carrier-type-spec": CarrierTypeSpec,
    "command": RuntimeCommand,
    "command-submission": CommandSubmission,
    "event": RuntimeEvent,
    "event-ref": EventRef,
    "gate": CarrierGate,
    "observation": Observation,
    "observation-kind": ObservationKindSpec,
    "process": CarrierProcess,
    "projection": Projection,
    "run": CarrierRun,
    "run-ref": RunRef,
    "runtime-budget": RuntimeBudget,
    "runtime-pool": RuntimePool,
    "runtime-ref": RuntimeRef,
}


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

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
            "archive-gc",
            "archive-run",
            "db",
            "create-run",
            "carrier-relations",
            "carrier-types",
            "carriers",
            "bridge",
            "doctor",
            "events",
            "export-bundle",
            "export-html",
            "gate",
            "init",
            "gates",
            "gc",
            "observations",
            "processes",
            "projections",
            "runs",
            "runtimes",
            "run-until-idle",
            "replay-execution",
            "diagnose-waits",
            "trace",
        }
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fala")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init", help="Initialize a local Carrier runtime workspace.")
    init.add_argument("--db", default=".fala/state.sqlite", help="Runtime SQLite DB path or sqlite:// URL.")
    init.add_argument("--artifact-root", default=".fala/artifacts", help="Filesystem artifact store root.")

    schema = subparsers.add_parser("schema", help="Emit JSON Schema for a Carrier runtime contract.")
    schema.add_argument("model", choices=sorted(CONTRACT_MODELS))

    db = subparsers.add_parser(
        "db",
        help="Initialize, migrate, and inspect Carrier runtime SQLite databases.",
    )
    db_subparsers = db.add_subparsers(dest="db_command", required=True)
    db_init = db_subparsers.add_parser("init", help="Create the Carrier SQLite schema.")
    _add_carrier_runtime_db_arg(db_init)
    db_migrate = db_subparsers.add_parser("migrate", help="Apply pending Carrier SQLite migrations.")
    _add_carrier_runtime_db_arg(db_migrate)
    db_status = db_subparsers.add_parser("status", help="Report Carrier SQLite schema status.")
    _add_carrier_runtime_db_arg(db_status)
    db_status.add_argument("--ensure-schema", action="store_true")
    db_vacuum = db_subparsers.add_parser("vacuum", help="Compact the Carrier SQLite database.")
    _add_carrier_runtime_db_arg(db_vacuum)

    gc = subparsers.add_parser("gc", help="Garbage-collect unreferenced filesystem artifact blobs.")
    gc.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    gc.add_argument("--artifact-root", default=".fala/artifacts", help="Filesystem artifact store root.")
    gc.add_argument("--run-id", default=None)
    gc.add_argument("--older-than", default=None, help="Only collect blobs older than duration like 30d, 12h, 20m.")
    gc.add_argument("--dry-run", action="store_true")

    archive_run = subparsers.add_parser("archive-run", help="Write a portable run archive bundle.")
    archive_run.add_argument("run_id")
    archive_run.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    archive_run.add_argument("--out", required=True, help="Output .zip path.")
    archive_run.add_argument("--retention-days", type=int, default=None, help="Record archive retention period in archive metadata.")

    archive_gc = subparsers.add_parser("archive-gc", help="Delete expired Fala run archive bundles.")
    archive_gc.add_argument("--archive-root", required=True, help="Directory containing .zip run archives.")
    archive_gc.add_argument("--dry-run", action="store_true")

    doctor = subparsers.add_parser("doctor", help="Check Carrier runtime readiness.")
    doctor.add_argument("--db", default=".fala/state.sqlite", help="Carrier runtime SQLite DB path or sqlite:// URL.")
    doctor.add_argument("--ensure-schema", action="store_true", help="Create/repair Carrier runtime schema before checking.")
    doctor.add_argument("--package", dest="packages", action="append", default=[], help="Carrier package YAML path to validate. Repeatable.")
    doctor.add_argument("--output", default=None, help="Write JSON doctor report to this path instead of stdout envelope.")

    create_run = subparsers.add_parser("create-run", help="Create a Carrier run.")
    _add_carrier_runtime_db_arg(create_run)
    create_run.add_argument("--run-id", default=None)
    create_run.add_argument("--title", default=None)
    create_run.add_argument("--package-id", default=None)
    create_run.add_argument("--package-version", default=None)
    create_run.add_argument("--package-digest", default=None)
    create_run.add_argument("--flow-id", default=None)
    create_run.add_argument("--flow-digest", default=None)
    create_run.add_argument("--runtime-version", default=None)
    create_run.add_argument("--backend-version", default=None)
    create_run.add_argument("--metadata", action="append", default=[])
    create_run.add_argument("--idempotency-key", default=None)

    runs = subparsers.add_parser("runs", help="Inspect Carrier runtime runs.")
    run_subparsers = runs.add_subparsers(dest="run_command", required=True)
    runs_list = run_subparsers.add_parser("list", help="List Carrier runs.")
    _add_carrier_runtime_db_arg(runs_list)
    runs_list.add_argument("--status", choices=[status.value for status in CarrierRunStatus], default=None)
    runs_list.add_argument("--limit", type=int, default=None)
    runs_list.add_argument("--jsonl", action="store_true")
    runs_inspect = run_subparsers.add_parser("inspect", help="Inspect one Carrier run.")
    _add_carrier_runtime_db_arg(runs_inspect)
    runs_inspect.add_argument("--run-id", required=True)
    runs_cancel = run_subparsers.add_parser("cancel", help="Request cancellation for one Carrier run.")
    _add_carrier_runtime_db_arg(runs_cancel)
    runs_cancel.add_argument("--run-id", required=True)
    runs_cancel.add_argument("--reason", default=None)
    runs_cancel.add_argument("--idempotency-key", default=None)

    commands = subparsers.add_parser("commands", help="Inspect runtime commands.")
    command_subparsers = commands.add_subparsers(
        dest="command_command",
        required=True,
    )
    commands_list = command_subparsers.add_parser("list", help="List runtime commands.")
    _add_carrier_runtime_db_run_args(commands_list)
    commands_list.add_argument("--command-type", default=None)
    commands_list.add_argument("--actor", default=None)
    commands_list.add_argument("--limit", type=int, default=None)
    commands_list.add_argument("--jsonl", action="store_true")
    commands_inspect = command_subparsers.add_parser(
        "inspect",
        help="Inspect one runtime command.",
    )
    _add_carrier_runtime_db_run_args(commands_inspect)
    commands_inspect.add_argument("--command-id", required=True)

    runtimes = subparsers.add_parser("runtimes", help="Inspect Carrier runtime pools.")
    runtime_subparsers = runtimes.add_subparsers(dest="runtime_command", required=True)
    runtimes_list = runtime_subparsers.add_parser("list", help="List runtime pools.")
    _add_carrier_runtime_db_arg(runtimes_list)
    runtimes_list.add_argument("--jsonl", action="store_true")
    runtimes_create = runtime_subparsers.add_parser("create-pool", help="Create or replace a runtime pool.")
    _add_carrier_runtime_db_arg(runtimes_create)
    runtimes_create.add_argument("--pool-id", required=True)
    runtimes_create.add_argument("--runtime-json", action="append", required=True, help="RuntimeRef JSON object. Repeatable.")
    runtimes_create.add_argument("--carrier-type", action="append", default=[])
    runtimes_create.add_argument("--policy", choices=["manual", "first", "least_busy", "round_robin"], default=None)
    runtimes_create.add_argument("--metadata-json", default="{}")
    runtimes_policy = runtime_subparsers.add_parser("add-policy", help="Create or replace a delegation policy.")
    _add_carrier_runtime_db_arg(runtimes_policy)
    runtimes_policy.add_argument("--policy-id", default=None)
    runtimes_policy.add_argument("--pool-id", required=True)
    runtimes_policy.add_argument("--carrier-type", action="append", default=[])
    runtimes_policy.add_argument("--budget-json", default="{}")
    runtimes_policy.add_argument("--metadata-json", default="{}")
    runtimes_inspect = runtime_subparsers.add_parser("inspect", help="Inspect one runtime pool.")
    _add_carrier_runtime_db_arg(runtimes_inspect)
    runtimes_inspect.add_argument("--pool-id", required=True)

    carriers = subparsers.add_parser("carriers", help="Inspect Carrier runtime carriers.")
    carrier_subparsers = carriers.add_subparsers(dest="carrier_command", required=True)
    carriers_create = carrier_subparsers.add_parser("create", help="Create a carrier.")
    _add_carrier_runtime_db_run_args(carriers_create)
    carriers_create.add_argument("--carrier-id", default=None)
    carriers_create.add_argument("--carrier-type", required=True)
    carriers_create.add_argument("--payload-json", default="{}")
    carriers_create.add_argument("--metadata-json", default="{}")
    carriers_create.add_argument("--idempotency-key", default=None)
    carriers_list = carrier_subparsers.add_parser("list", help="List carriers.")
    _add_carrier_runtime_db_run_args(carriers_list)
    carriers_list.add_argument("--carrier-type", default=None)
    carriers_list.add_argument("--limit", type=int, default=None)
    carriers_list.add_argument("--jsonl", action="store_true")
    carriers_inspect = carrier_subparsers.add_parser("inspect", help="Inspect one carrier.")
    _add_carrier_runtime_db_run_args(carriers_inspect)
    carriers_inspect.add_argument("--carrier-id", required=True)

    carrier_types = subparsers.add_parser("carrier-types", help="Inspect Carrier type metadata.")
    carrier_type_subparsers = carrier_types.add_subparsers(dest="carrier_type_command", required=True)
    carrier_types_list = carrier_type_subparsers.add_parser("list", help="List carrier types.")
    _add_carrier_runtime_db_run_args(carrier_types_list)
    carrier_types_list.add_argument("--jsonl", action="store_true")
    carrier_types_inspect = carrier_type_subparsers.add_parser("inspect", help="Inspect one carrier type.")
    _add_carrier_runtime_db_run_args(carrier_types_inspect)
    carrier_types_inspect.add_argument("--carrier-type-id", required=True)

    carrier_relations = subparsers.add_parser("carrier-relations", help="Inspect Carrier relations.")
    carrier_relation_subparsers = carrier_relations.add_subparsers(dest="carrier_relation_command", required=True)
    carrier_relations_list = carrier_relation_subparsers.add_parser("list", help="List carrier relations.")
    _add_carrier_runtime_db_run_args(carrier_relations_list)
    carrier_relations_list.add_argument("--carrier-id", default=None)
    carrier_relations_list.add_argument("--relation-type", default=None)
    carrier_relations_list.add_argument("--jsonl", action="store_true")
    carrier_relations_inspect = carrier_relation_subparsers.add_parser("inspect", help="Inspect one carrier relation.")
    _add_carrier_runtime_db_run_args(carrier_relations_inspect)
    carrier_relations_inspect.add_argument("--relation-id", required=True)

    artifacts = subparsers.add_parser("artifacts", help="Inspect Carrier artifact metadata.")
    artifact_subparsers = artifacts.add_subparsers(dest="artifact_command", required=True)
    artifacts_record = artifact_subparsers.add_parser("record", help="Record one filesystem artifact.")
    _add_carrier_runtime_db_run_args(artifacts_record)
    artifacts_record.add_argument("--artifact-root", default=".fala/artifacts")
    artifacts_record.add_argument("--path", required=True)
    artifacts_record.add_argument("--kind", required=True)
    artifacts_record.add_argument("--artifact-id", default=None)
    artifacts_record.add_argument("--carrier-id", default=None)
    artifacts_record.add_argument("--media-type", default=None)
    artifacts_record.add_argument("--metadata-json", default="{}")
    artifacts_record.add_argument("--idempotency-key", default=None)
    artifacts_list = artifact_subparsers.add_parser("list", help="List artifacts.")
    _add_carrier_runtime_db_run_args(artifacts_list)
    artifacts_list.add_argument("--carrier-id", default=None)
    artifacts_list.add_argument("--kind", default=None)
    artifacts_list.add_argument("--jsonl", action="store_true")
    artifacts_inspect = artifact_subparsers.add_parser("inspect", help="Inspect one artifact.")
    _add_carrier_runtime_db_run_args(artifacts_inspect)
    artifacts_inspect.add_argument("--artifact-id", required=True)

    processes = subparsers.add_parser("processes", help="Inspect Carrier runtime processes.")
    process_subparsers = processes.add_subparsers(dest="process_command", required=True)
    processes_schedule = process_subparsers.add_parser("schedule", help="Schedule a carrier process.")
    _add_carrier_runtime_db_run_args(processes_schedule)
    processes_schedule.add_argument("--process-id", default=None)
    processes_schedule.add_argument("--carrier-id", default=None)
    processes_schedule.add_argument("--process-type", required=True)
    processes_schedule.add_argument("--status", choices=["pending", "ready"], default="ready")
    processes_schedule.add_argument("--priority", type=int, default=0)
    processes_schedule.add_argument("--max-attempts", type=int, default=1)
    processes_schedule.add_argument("--input-json", default="{}")
    processes_schedule.add_argument("--metadata-json", default="{}")
    processes_schedule.add_argument("--idempotency-key", default=None)
    processes_list = process_subparsers.add_parser("list", help="List processes.")
    _add_carrier_runtime_db_run_args(processes_list)
    processes_list.add_argument("--status", choices=[status.value for status in CarrierProcessStatus], default=None)
    processes_list.add_argument("--carrier-id", default=None)
    processes_list.add_argument("--jsonl", action="store_true")
    processes_inspect = process_subparsers.add_parser("inspect", help="Inspect one process.")
    _add_carrier_runtime_db_run_args(processes_inspect)
    processes_inspect.add_argument("--process-id", required=True)
    processes_cancel = process_subparsers.add_parser("cancel", help="Cancel one process.")
    _add_carrier_runtime_db_run_args(processes_cancel)
    processes_cancel.add_argument("--process-id", required=True)
    processes_cancel.add_argument("--error-json", default="{}")
    processes_cancel.add_argument("--idempotency-key", default=None)
    processes_timeout = process_subparsers.add_parser("timeout", help="Mark one process timed out.")
    _add_carrier_runtime_db_run_args(processes_timeout)
    processes_timeout.add_argument("--process-id", required=True)
    processes_timeout.add_argument("--error-json", default="{}")
    processes_timeout.add_argument("--idempotency-key", default=None)

    observations = subparsers.add_parser("observations", help="Inspect Carrier observations.")
    observation_subparsers = observations.add_subparsers(dest="observation_command", required=True)
    observations_append = observation_subparsers.add_parser("append", help="Append one observation.")
    _add_carrier_runtime_db_run_args(observations_append)
    observations_append.add_argument("--observation-id", default=None)
    observations_append.add_argument("--carrier-id", default=None)
    observations_append.add_argument("--kind", required=True)
    observations_append.add_argument("--values-json", default="{}")
    observations_append.add_argument("--metadata-json", default="{}")
    observations_append.add_argument("--idempotency-key", default=None)
    observations_list = observation_subparsers.add_parser("list", help="List observations.")
    _add_carrier_runtime_db_run_args(observations_list)
    observations_list.add_argument("--carrier-id", default=None)
    observations_list.add_argument("--jsonl", action="store_true")
    observations_inspect = observation_subparsers.add_parser("inspect", help="Inspect one observation.")
    _add_carrier_runtime_db_run_args(observations_inspect)
    observations_inspect.add_argument("--observation-id", required=True)

    events = subparsers.add_parser("events", help="Inspect Carrier runtime events.")
    event_subparsers = events.add_subparsers(dest="event_command", required=True)
    events_list = event_subparsers.add_parser("list", help="List ordered events.")
    _add_carrier_runtime_db_run_args(events_list)
    events_list.add_argument("--carrier-id", default=None)
    events_list.add_argument("--after-sequence", type=int, default=None)
    events_list.add_argument("--limit", type=int, default=None)
    events_list.add_argument("--jsonl", action="store_true")
    events_validate = event_subparsers.add_parser(
        "validate-schema",
        help="Validate event schema versions for a run.",
    )
    _add_carrier_runtime_db_run_args(events_validate)
    events_validate.add_argument("--max-schema-version", type=int, default=1)

    gates = subparsers.add_parser("gates", help="Inspect Carrier runtime gates.")
    gate_subparsers = gates.add_subparsers(dest="gate_command", required=True)
    gates_list = gate_subparsers.add_parser("list", help="List gates.")
    _add_carrier_runtime_db_run_args(gates_list)
    gates_list.add_argument("--carrier-id", default=None)
    gates_list.add_argument("--status", choices=[status.value for status in CarrierGateStatus], default=None)
    gates_list.add_argument("--jsonl", action="store_true")

    gate = subparsers.add_parser("gate", help="Mutate one Carrier runtime gate.")
    single_gate_subparsers = gate.add_subparsers(dest="gate_command", required=True)
    gate_open = single_gate_subparsers.add_parser("open", help="Open a gate.")
    _add_carrier_runtime_db_run_args(gate_open)
    gate_open.add_argument("--gate-id", default=None)
    gate_open.add_argument("--carrier-id", default=None)
    gate_open.add_argument("--kind", required=True)
    gate_open.add_argument("--values-json", default="{}")
    gate_open.add_argument("--metadata-json", default="{}")
    gate_open.add_argument("--idempotency-key", default=None)
    gate_complete = single_gate_subparsers.add_parser("complete", help="Complete an open gate.")
    _add_carrier_runtime_db_run_args(gate_complete)
    gate_complete.add_argument("--gate-id", required=True)
    gate_complete.add_argument("--value", action="append", default=[], help="Gate output value as key=value. Repeatable.")
    gate_complete.add_argument("--idempotency-key", default=None)
    gate_cancel = single_gate_subparsers.add_parser("cancel", help="Cancel an open gate.")
    _add_carrier_runtime_db_run_args(gate_cancel)
    gate_cancel.add_argument("--gate-id", required=True)
    gate_cancel.add_argument("--value", action="append", default=[], help="Gate output value as key=value. Repeatable.")
    gate_cancel.add_argument("--idempotency-key", default=None)
    gate_expire = single_gate_subparsers.add_parser("expire", help="Expire an open gate.")
    _add_carrier_runtime_db_run_args(gate_expire)
    gate_expire.add_argument("--gate-id", required=True)
    gate_expire.add_argument("--value", action="append", default=[], help="Gate output value as key=value. Repeatable.")
    gate_expire.add_argument("--idempotency-key", default=None)

    projections = subparsers.add_parser("projections", help="Inspect Carrier projections.")
    projection_subparsers = projections.add_subparsers(dest="projection_command", required=True)
    projections_list = projection_subparsers.add_parser("list", help="List projections.")
    _add_carrier_runtime_db_run_args(projections_list)
    projections_list.add_argument("--jsonl", action="store_true")
    projections_rebuild = projection_subparsers.add_parser("rebuild", help="Rebuild Carrier projections.")
    _add_carrier_runtime_db_run_args(projections_rebuild)
    projections_rebuild.add_argument("--name", action="append", default=[], help="Projection name to rebuild. Repeatable. Defaults to all built-ins.")
    projections_rebuild.add_argument("--idempotency-key", default=None)
    projections_rebuild.add_argument("--jsonl", action="store_true")

    bridge = subparsers.add_parser("bridge", help="Inspect and deliver Carrier runtime bridge records.")
    bridge_subparsers = bridge.add_subparsers(dest="bridge_command", required=True)
    bridge_list = bridge_subparsers.add_parser("list", help="List bridge deliveries.")
    _add_carrier_runtime_db_run_args(bridge_list)
    bridge_list.add_argument("--box", choices=("outbox", "inbox"), default="outbox")
    bridge_list.add_argument("--status", choices=[status.value for status in BridgeDeliveryStatus], default=None)
    bridge_list.add_argument("--jsonl", action="store_true")
    bridge_deliver = bridge_subparsers.add_parser("deliver", help="Deliver one outbox record into another local SQLite runtime.")
    _add_carrier_runtime_db_run_args(bridge_deliver)
    bridge_deliver.add_argument("--delivery-id", required=True)
    bridge_deliver.add_argument("--target-db", required=True)
    bridge_deliver.add_argument("--idempotency-key", default=None)
    bridge_deliver.add_argument("--import-idempotency-key", default=None)
    bridge_export = bridge_subparsers.add_parser("export", help="Export one outbox bridge delivery to JSON.")
    _add_carrier_runtime_db_run_args(bridge_export)
    bridge_export.add_argument("--delivery-id", required=True)
    bridge_export.add_argument("--out", required=True)
    bridge_import = bridge_subparsers.add_parser("import", help="Import one bridge delivery JSON file.")
    _add_carrier_runtime_db_arg(bridge_import)
    bridge_import.add_argument("--file", required=True)
    bridge_import.add_argument("--idempotency-key", default=None)

    run_until_idle = subparsers.add_parser("run-until-idle", help="Run ready Carrier processes until idle.")
    run_until_idle.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    run_until_idle.add_argument("--run-id", default=None)
    run_until_idle.add_argument("--worker-id", default="cli:run-until-idle")
    run_until_idle.add_argument("--lease-seconds", type=float, default=300.0)
    run_until_idle.add_argument("--max-ticks", type=int, default=100)
    run_until_idle.add_argument("--work-dir", default=None)

    replay_execution = subparsers.add_parser("replay-execution", help="Replay or verify a recorded Carrier process execution.")
    replay_execution.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    replay_execution.add_argument("--run-id", required=True)
    replay_execution.add_argument("--process-id", required=True)
    replay_execution.add_argument("--rerun", action="store_true", help="Rerun only if process metadata marks it deterministic.")
    replay_execution.add_argument("--work-dir", default=None)

    diagnose_waits = subparsers.add_parser("diagnose-waits", help="Diagnose Carrier waits and wait-graph deadlocks.")
    diagnose_waits.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    diagnose_waits.add_argument("--run-id", required=True)
    diagnose_waits.add_argument("--carrier-id", default=None)

    trace = subparsers.add_parser("trace", help="Show Carrier runtime trace for one run.")
    trace.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    trace.add_argument("--run-id", required=True)

    export_html = subparsers.add_parser("export-html", help="Export a static Carrier runtime HTML report.")
    export_html.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    export_html.add_argument("--run-id", required=True)
    export_html.add_argument("--out", required=True, help="Output HTML path.")

    export_bundle = subparsers.add_parser("export-bundle", help="Export a portable Carrier runtime debug bundle.")
    export_bundle.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")
    export_bundle.add_argument("--run-id", required=True)
    export_bundle.add_argument("--out", required=True, help="Output .zip path.")

    return parser

async def _run(args: argparse.Namespace) -> dict[str, Any] | None:
    if args.command == "init":
        db_path = Path(_carrier_runtime_db_path(args.db))
        artifact_root = Path(args.artifact_root).expanduser()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_root.mkdir(parents=True, exist_ok=True)
        SQLiteRuntimeBackend(db_path)
        return {
            "ok": True,
            "db": str(db_path),
            "artifact_root": str(artifact_root),
            "schema_version": SQLITE_RUNTIME_SCHEMA_VERSION,
        }

    if args.command == "schema":
        model = CONTRACT_MODELS[args.model]
        return {
            "ok": True,
            "model": args.model,
            "schema": model.model_json_schema(),
        }

    if args.command == "db":
        db_path = _carrier_runtime_db_path(args.db)
        backend = SQLiteRuntimeBackend(db_path)
        if args.db_command in {"init", "migrate"}:
            return {
                "ok": True,
                "path": str(db_path),
                "schema_version": SQLITE_RUNTIME_SCHEMA_VERSION,
            }
        if args.db_command == "vacuum":
            return _carrier_runtime_vacuum(db_path)
        return _carrier_runtime_doctor(
            argparse.Namespace(
                db=args.db,
                ensure_schema=args.ensure_schema,
                packages=[],
                output=None,
            )
        )

    if args.command == "doctor":
        return _carrier_runtime_doctor(args)

    if args.command in {
        "archive-gc",
        "archive-run",
        "artifacts",
        "bridge",
        "create-run",
        "carrier-relations",
        "carrier-types",
        "carriers",
        "commands",
        "diagnose-waits",
        "events",
        "export-bundle",
        "export-html",
        "gate",
        "gates",
        "gc",
        "observations",
        "processes",
        "projections",
        "runtimes",
        "run-until-idle",
        "replay-execution",
        "runs",
        "trace",
    }:
        return await _carrier_runtime_command(args)

    raise ValueError(f"Unknown Fala command: {args.command}")

_CARRIER_RUNTIME_REQUIRED_TABLES = (
    "artifacts",
    "bridge_inbox",
    "bridge_outbox",
    "carrier_relations",
    "carrier_types",
    "carriers",
    "delegation_policies",
    "gates",
    "observations",
    "processes",
    "projections",
    "runtime_pools",
    "runtime_commands",
    "runtime_events",
    "runs",
    "schema_migrations",
)


async def _carrier_runtime_command(args: argparse.Namespace) -> dict[str, Any] | None:
    if args.command == "diagnose-waits":
        service = RuntimeBackendService(
            SQLiteRuntimeBackend(_carrier_runtime_db_path(args.db))
        )
        diagnostic = await service.diagnose_waits(
            run_id=args.run_id,
            carrier_id=args.carrier_id,
        )
        return {
            "ok": True,
            "wait_diagnostics": diagnostic.model_dump(mode="json"),
        }
    if args.command == "trace":
        return await _carrier_runtime_trace(args)
    if args.command == "archive-gc":
        return _carrier_runtime_archive_gc(args)
    if args.command == "archive-run":
        return await _carrier_runtime_archive_run(args)
    if args.command == "export-html":
        return await _carrier_runtime_export_html(args)
    if args.command == "export-bundle":
        return await _carrier_runtime_export_bundle(args)
    if args.command == "gc":
        return await _carrier_runtime_gc(args)
    if args.command == "run-until-idle":
        return await _carrier_runtime_run_until_idle(args)
    if args.command == "replay-execution":
        return await _carrier_runtime_replay_execution(args)

    backend = SQLiteRuntimeBackend(_carrier_runtime_db_path(args.db))
    if args.command == "create-run":
        run_data = {
            "title": args.title,
            "package_id": args.package_id,
            "package_version": args.package_version,
            "package_digest": args.package_digest,
            "flow_id": args.flow_id,
            "flow_digest": args.flow_digest,
            "runtime_version": args.runtime_version,
            "backend_version": args.backend_version,
            "metadata": _parse_values(args.metadata),
        }
        if args.run_id is not None:
            run_data["id"] = args.run_id
        run = CarrierRun.model_validate(run_data)
        service = RuntimeBackendService(backend)
        stored, submission = await service.create_run(
            run,
            idempotency_key=args.idempotency_key or f"{run.id}:run.create",
            actor="cli:user",
        )
        return {
            "ok": True,
            "run": stored.model_dump(mode="json"),
            "command": submission.command.model_dump(mode="json"),
            "replayed": submission.replayed,
        }
    if args.command == "runs":
        if args.run_command == "list":
            runs = await backend.list_runs(
                status=CarrierRunStatus(args.status) if args.status else None,
                limit=args.limit,
            )
            return _carrier_runtime_list_result("runs", runs, jsonl=args.jsonl)
        if args.run_command == "cancel":
            service = RuntimeBackendService(backend)
            run, submission = await service.cancel_run(
                run_id=args.run_id,
                idempotency_key=args.idempotency_key or f"{args.run_id}:run.cancel",
                reason=args.reason,
                actor="cli:user",
            )
            return {
                "ok": True,
                "run": run.model_dump(mode="json"),
                "command": submission.command.model_dump(mode="json"),
                "replayed": submission.replayed,
            }
        run = await backend.get_run(run_id=args.run_id)
        return {
            "ok": run is not None,
            "run": run.model_dump(mode="json") if run is not None else None,
        }
    if args.command == "commands":
        if args.command_command == "inspect":
            command = await backend.get_command(
                run_id=args.run_id,
                command_id=args.command_id,
            )
            return {
                "ok": command is not None,
                "command": command.model_dump(mode="json")
                if command is not None
                else None,
            }
        commands = await backend.list_commands(
            run_id=args.run_id,
            command_type=args.command_type,
            actor=args.actor,
            limit=args.limit,
        )
        return _carrier_runtime_list_result(
            "commands",
            commands,
            jsonl=args.jsonl,
        )
    if args.command == "runtimes":
        service = RuntimeBackendService(backend)
        if args.runtime_command == "list":
            pools = await backend.list_runtime_pools()
            return _carrier_runtime_list_result(
                "runtime_pools",
                pools,
                jsonl=args.jsonl,
            )
        if args.runtime_command == "create-pool":
            metadata = _parse_json_object(args.metadata_json, "--metadata-json")
            if args.policy is not None:
                metadata["policy"] = args.policy
            pool = RuntimePool(
                id=args.pool_id,
                runtimes=[
                    RuntimeRef.model_validate(
                        _parse_json_object(value, "--runtime-json")
                    )
                    for value in args.runtime_json
                ],
                carrier_types=args.carrier_type,
                metadata=metadata,
            )
            stored = await service.save_runtime_pool(pool)
            return {
                "ok": True,
                "runtime_pool": stored.model_dump(mode="json"),
            }
        if args.runtime_command == "add-policy":
            policy_data = {
                "pool_id": args.pool_id,
                "carrier_types": args.carrier_type,
                "budget": _parse_json_object(args.budget_json, "--budget-json"),
                "metadata": _parse_json_object(args.metadata_json, "--metadata-json"),
            }
            if args.policy_id is not None:
                policy_data["id"] = args.policy_id
            stored = await service.save_delegation_policy(
                DelegationPolicy.model_validate(policy_data)
            )
            return {
                "ok": True,
                "delegation_policy": stored.model_dump(mode="json"),
            }
        pool = await backend.get_runtime_pool(pool_id=args.pool_id)
        policies = await backend.list_delegation_policies(pool_id=args.pool_id)
        return {
            "ok": pool is not None,
            "runtime_pool": pool.model_dump(mode="json") if pool is not None else None,
            "delegation_policies": [
                policy.model_dump(mode="json") for policy in policies
            ],
        }
    if args.command == "carriers":
        if args.carrier_command == "create":
            carrier_data = {
                "run_id": args.run_id,
                "carrier_type": args.carrier_type,
                "payload": _parse_json_object(args.payload_json, "--payload-json"),
                "metadata": _parse_json_object(args.metadata_json, "--metadata-json"),
            }
            if args.carrier_id is not None:
                carrier_data["id"] = args.carrier_id
            carrier = Carrier.model_validate(carrier_data)
            service = RuntimeBackendService(backend)
            stored, submission = await service.accept_carrier(
                carrier,
                idempotency_key=args.idempotency_key
                or f"{carrier.run_id}:carrier.accept:{carrier.id}",
                actor="cli:user",
            )
            return {
                "ok": True,
                "carrier": stored.model_dump(mode="json"),
                "command": submission.command.model_dump(mode="json"),
                "replayed": submission.replayed,
            }
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
    if args.command == "bridge":
        service = RuntimeBackendService(backend)
        if args.bridge_command == "list":
            status = BridgeDeliveryStatus(args.status) if args.status else None
            deliveries = (
                await service.list_outbox_deliveries(
                    run_id=args.run_id,
                    status=status,
                )
                if args.box == "outbox"
                else await service.list_inbox_deliveries(
                    run_id=args.run_id,
                    status=status,
                )
            )
            return _carrier_runtime_list_result(
                f"bridge_{args.box}",
                deliveries,
                jsonl=args.jsonl,
            )
        if args.bridge_command == "export":
            delivery = await service.backend.get_outbox_delivery(
                run_id=args.run_id,
                delivery_id=args.delivery_id,
            )
            if delivery is None:
                return {
                    "ok": False,
                    "run_id": args.run_id,
                    "delivery_id": args.delivery_id,
                    "error": "outbox delivery not found",
                }
            out = Path(args.out).expanduser()
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(
                json.dumps(delivery.model_dump(mode="json"), indent=2, sort_keys=True)
                + "\n",
                encoding="utf-8",
            )
            return {
                "ok": True,
                "run_id": args.run_id,
                "delivery_id": args.delivery_id,
                "out": str(out),
            }
        if args.bridge_command == "import":
            path = Path(args.file).expanduser()
            delivery = BridgeDelivery.model_validate(
                json.loads(path.read_text(encoding="utf-8"))
            )
            imported, submission = await service.import_bridge_delivery(
                delivery,
                idempotency_key=args.idempotency_key
                or f"{delivery.target.run_id}:bridge.file.import:{delivery.id}",
                actor="cli:user",
            )
            return {
                "ok": True,
                "imported": imported.model_dump(mode="json"),
                "command": submission.command.model_dump(mode="json"),
                "replayed": submission.replayed,
            }
        target = RuntimeBackendService.sqlite(_carrier_runtime_db_path(args.target_db))
        delivered, imported, delivery_submission, import_submission = (
            await service.deliver_bridge_delivery(
                run_id=args.run_id,
                delivery_id=args.delivery_id,
                target=target,
                idempotency_key=args.idempotency_key
                or f"{args.run_id}:bridge.deliver:{args.delivery_id}",
                import_idempotency_key=args.import_idempotency_key,
                actor="cli:user",
            )
        )
        return {
            "ok": True,
            "delivered": delivered.model_dump(mode="json"),
            "imported": imported.model_dump(mode="json"),
            "delivery_command": delivery_submission.command.model_dump(mode="json"),
            "import_command": import_submission.command.model_dump(mode="json"),
            "delivery_replayed": delivery_submission.replayed,
            "import_replayed": import_submission.replayed,
        }
    if args.command == "artifacts":
        if args.artifact_command == "record":
            store = FileArtifactStore(args.artifact_root)
            ref = store.put_file(
                kind=args.kind,
                path=args.path,
                artifact_id=args.artifact_id,
                metadata=_parse_json_object(args.metadata_json, "--metadata-json"),
            )
            digest = ref.metadata.get("sha256")
            artifact = CarrierArtifact(
                id=ref.id,
                run_id=args.run_id,
                carrier_id=args.carrier_id,
                kind=args.kind,
                uri=ref.uri,
                media_type=args.media_type,
                size_bytes=ref.metadata.get("size_bytes"),
                content_hash=f"sha256:{digest}" if isinstance(digest, str) else None,
                metadata={**ref.metadata, "artifact_store": store.location},
            )
            service = RuntimeBackendService(backend)
            stored, submission = await service.record_artifact(
                artifact,
                idempotency_key=args.idempotency_key
                or f"{args.run_id}:artifact.record:{artifact.id}",
                actor="cli:user",
            )
            return {
                "ok": True,
                "artifact": stored.model_dump(mode="json"),
                "command": submission.command.model_dump(mode="json"),
                "replayed": submission.replayed,
            }
        if args.artifact_command == "list":
            artifacts = await backend.list_artifacts(
                run_id=args.run_id,
                carrier_id=args.carrier_id,
                kind=args.kind,
            )
            return _carrier_runtime_list_result(
                "artifacts",
                artifacts,
                jsonl=args.jsonl,
            )
        artifact = await backend.get_artifact(
            run_id=args.run_id,
            artifact_id=args.artifact_id,
        )
        return {
            "ok": artifact is not None,
            "artifact": artifact.model_dump(mode="json")
            if artifact is not None
            else None,
        }
    if args.command == "processes":
        if args.process_command == "schedule":
            process_data = {
                "run_id": args.run_id,
                "carrier_id": args.carrier_id,
                "process_type": args.process_type,
                "status": CarrierProcessStatus(args.status),
                "priority": args.priority,
                "max_attempts": args.max_attempts,
                "input": _parse_json_object(args.input_json, "--input-json"),
                "metadata": _parse_json_object(args.metadata_json, "--metadata-json"),
            }
            if args.process_id is not None:
                process_data["id"] = args.process_id
            process = CarrierProcess.model_validate(process_data)
            service = RuntimeBackendService(backend)
            stored, submission = await service.schedule_process(
                process,
                idempotency_key=args.idempotency_key
                or f"{process.run_id}:process.schedule:{process.id}",
                actor="cli:user",
            )
            return {
                "ok": True,
                "process": stored.model_dump(mode="json"),
                "command": submission.command.model_dump(mode="json"),
                "replayed": submission.replayed,
            }
        if args.process_command in {"cancel", "timeout"}:
            service = RuntimeBackendService(backend)
            method = {
                "cancel": service.cancel_process,
                "timeout": service.timeout_process,
            }[args.process_command]
            stored, submission = await method(
                run_id=args.run_id,
                process_id=args.process_id,
                error=_parse_json_object(args.error_json, "--error-json"),
                idempotency_key=args.idempotency_key
                or f"{args.run_id}:process.{args.process_command}:{args.process_id}",
                actor="cli:user",
            )
            return {
                "ok": True,
                "process": stored.model_dump(mode="json"),
                "command": submission.command.model_dump(mode="json"),
                "replayed": submission.replayed,
            }
        if args.process_command == "list":
            processes = await backend.list_processes(
                run_id=args.run_id,
                status=CarrierProcessStatus(args.status) if args.status else None,
                carrier_id=args.carrier_id,
            )
            return _carrier_runtime_list_result(
                "processes",
                processes,
                jsonl=args.jsonl,
            )
        process = await backend.get_process(
            run_id=args.run_id,
            process_id=args.process_id,
        )
        return {
            "ok": process is not None,
            "process": process.model_dump(mode="json")
            if process is not None
            else None,
        }
    if args.command == "observations":
        if args.observation_command == "append":
            observation_data = {
                "run_id": args.run_id,
                "carrier_id": args.carrier_id,
                "kind": args.kind,
                "values": _parse_json_object(args.values_json, "--values-json"),
                "metadata": _parse_json_object(args.metadata_json, "--metadata-json"),
            }
            if args.observation_id is not None:
                observation_data["id"] = args.observation_id
            observation = Observation.model_validate(observation_data)
            service = RuntimeBackendService(backend)
            stored, submission = await service.record_observation(
                observation,
                idempotency_key=args.idempotency_key
                or f"{observation.run_id}:observation.record:{observation.id}",
                actor="cli:user",
            )
            return {
                "ok": True,
                "observation": stored.model_dump(mode="json"),
                "command": submission.command.model_dump(mode="json"),
                "replayed": submission.replayed,
            }
        if args.observation_command == "inspect":
            observations = await backend.list_observations(run_id=args.run_id)
            observation = next(
                (
                    item
                    for item in observations
                    if item.id == args.observation_id
                ),
                None,
            )
            return {
                "ok": observation is not None,
                "observation": observation.model_dump(mode="json")
                if observation is not None
                else None,
            }
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
        if args.event_command == "validate-schema":
            events = await backend.list_events(run_id=args.run_id)
            return _carrier_runtime_event_schema_report(
                events,
                max_schema_version=args.max_schema_version,
            )
        events = await backend.list_events(
            run_id=args.run_id,
            carrier_id=args.carrier_id,
            after_sequence=args.after_sequence,
            limit=args.limit,
        )
        return _carrier_runtime_list_result("events", events, jsonl=args.jsonl)
    if args.command in {"gate", "gates"}:
        if args.gate_command == "open":
            gate_data = {
                "run_id": args.run_id,
                "carrier_id": args.carrier_id,
                "kind": args.kind,
                "values": _parse_json_object(args.values_json, "--values-json"),
                "metadata": _parse_json_object(args.metadata_json, "--metadata-json"),
            }
            if args.gate_id is not None:
                gate_data["id"] = args.gate_id
            gate = CarrierGate.model_validate(gate_data)
            service = RuntimeBackendService(backend)
            opened, submission = await service.open_gate(
                gate,
                idempotency_key=args.idempotency_key
                or f"{args.run_id}:gate.open:{gate.id}",
                actor="cli:user",
            )
            return {
                "ok": True,
                "gate": opened.model_dump(mode="json"),
                "command": submission.command.model_dump(mode="json"),
                "replayed": submission.replayed,
            }
        if args.gate_command in {"complete", "cancel", "expire"}:
            service = RuntimeBackendService(backend)
            method = {
                "complete": service.complete_gate,
                "cancel": service.cancel_gate,
                "expire": service.expire_gate,
            }[args.gate_command]
            gate, submission = await method(
                run_id=args.run_id,
                gate_id=args.gate_id,
                values=_parse_values(args.value),
                idempotency_key=args.idempotency_key
                or f"{args.run_id}:gate.{args.gate_command}:{args.gate_id}",
                actor="cli:user",
            )
            return {
                "ok": True,
                "gate": gate.model_dump(mode="json"),
                "command": submission.command.model_dump(mode="json"),
                "replayed": submission.replayed,
            }
        gates = await backend.list_gates(
            run_id=args.run_id,
            carrier_id=args.carrier_id,
            status=CarrierGateStatus(args.status) if args.status else None,
        )
        return _carrier_runtime_list_result("gates", gates, jsonl=args.jsonl)
    if args.command == "projections":
        if args.projection_command == "rebuild":
            service = RuntimeBackendService(backend)
            names = args.name or None
            rebuilt, submission = await service.rebuild_projections(
                run_id=args.run_id,
                names=names,
                idempotency_key=args.idempotency_key
                or f"{args.run_id}:projection.rebuild:{','.join(args.name) if args.name else 'all'}",
                actor="cli:user",
            )
            if args.jsonl:
                return _carrier_runtime_list_result(
                    "projections",
                    rebuilt,
                    jsonl=True,
                )
            return {
                "ok": True,
                "count": len(rebuilt),
                "projections": [
                    projection.model_dump(mode="json") for projection in rebuilt
                ],
                "command": submission.command.model_dump(mode="json"),
                "replayed": submission.replayed,
            }
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


def _carrier_runtime_event_schema_report(
    events: list[RuntimeEvent],
    *,
    max_schema_version: int,
) -> dict[str, Any]:
    if max_schema_version < 1:
        raise ValueError("--max-schema-version must be greater than zero")
    versions: dict[str, int] = {}
    unsupported: list[dict[str, Any]] = []
    for event in events:
        version = str(event.schema_version)
        versions[version] = versions.get(version, 0) + 1
        if event.schema_version > max_schema_version:
            unsupported.append(
                {
                    "id": event.id,
                    "sequence": event.sequence,
                    "event_type": event.event_type,
                    "schema_version": event.schema_version,
                }
            )
    return {
        "ok": not unsupported,
        "event_count": len(events),
        "max_schema_version": max_schema_version,
        "schema_versions": dict(
            sorted(versions.items(), key=lambda item: int(item[0]))
        ),
        "unsupported_events": unsupported,
    }


async def _carrier_runtime_run_until_idle(args: argparse.Namespace) -> dict[str, Any]:
    if args.max_ticks < 1:
        raise ValueError("--max-ticks must be greater than zero")
    if args.lease_seconds <= 0:
        raise ValueError("--lease-seconds must be greater than zero")
    backend = SQLiteRuntimeBackend(_carrier_runtime_db_path(args.db))
    service = RuntimeBackendService(backend)
    completed: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    waiting: list[dict[str, Any]] = []
    ticks = 0
    work_root = Path(args.work_dir).expanduser() if args.work_dir else None
    if work_root is not None:
        work_root.mkdir(parents=True, exist_ok=True)

    while ticks < args.max_ticks:
        process = await service.claim_next_ready_process(
            worker_id=args.worker_id,
            run_id=args.run_id,
            lease_seconds=args.lease_seconds,
        )
        if process is None:
            break
        ticks += 1
        try:
            adapter, step_input, config = _process_step_request_parts(process)
            step_work_dir = work_root / process.id if work_root is not None else None
            if step_work_dir is not None:
                step_work_dir.mkdir(parents=True, exist_ok=True)
            request = StepRunRequest(
                run_id=process.run_id,
                process_id=process.id,
                carrier_id=process.carrier_id,
                adapter=adapter,
                input=step_input,
                config=config,
                work_dir=step_work_dir,
            )
            if adapter.kind == "fala_runtime":
                result = await _carrier_runtime_enqueue_fala_runtime_process(
                    backend=backend,
                    service=service,
                    process=process,
                    request=request,
                    db_path=_carrier_runtime_db_path(args.db),
                    actor=args.worker_id,
                )
            else:
                result = await create_step_adapter(adapter.kind).run(request)
            if result.waiting:
                if result.gate_id is not None:
                    await service.save_gate(
                        CarrierGate(
                            id=result.gate_id,
                            run_id=process.run_id,
                            carrier_id=process.carrier_id,
                            kind=adapter.kind,
                            values=result.output,
                            metadata=result.metadata,
                        ),
                        idempotency_key=f"{process.run_id}:gate.open:{result.gate_id}",
                        actor=args.worker_id,
                    )
                stored, _ = await service.wait_process(
                    run_id=process.run_id,
                    process_id=process.id,
                    output=result.output,
                    idempotency_key=f"{process.run_id}:process.wait:{process.id}:{process.attempt}",
                    actor=args.worker_id,
                )
                waiting.append(stored.model_dump(mode="json"))
                continue

            stored, _ = await service.complete_process(
                run_id=process.run_id,
                process_id=process.id,
                output={
                    **result.output,
                    "adapter": {
                        "returncode": result.returncode,
                        "stdout": result.stdout,
                        "stderr": result.stderr,
                    },
                },
                idempotency_key=f"{process.run_id}:process.complete:{process.id}:{process.attempt}",
                actor=args.worker_id,
            )
            completed.append(stored.model_dump(mode="json"))
        except Exception as exc:
            error = {"type": type(exc).__name__, "message": str(exc)}
            if process.attempt < process.max_attempts:
                stored, _ = await service.retry_process(
                    run_id=process.run_id,
                    process_id=process.id,
                    error=error,
                    idempotency_key=f"{process.run_id}:process.retry:{process.id}:{process.attempt}",
                    actor=args.worker_id,
                )
            else:
                stored, _ = await service.fail_process(
                    run_id=process.run_id,
                    process_id=process.id,
                    error=error,
                    idempotency_key=f"{process.run_id}:process.fail:{process.id}:{process.attempt}",
                    actor=args.worker_id,
                )
            failed.append(stored.model_dump(mode="json"))

    return {
        "ok": ticks < args.max_ticks,
        "ticks": ticks,
        "stopped_reason": "max_ticks" if ticks >= args.max_ticks else "idle",
        "completed": completed,
        "failed": failed,
        "waiting": waiting,
    }


async def _carrier_runtime_replay_execution(args: argparse.Namespace) -> dict[str, Any]:
    backend = SQLiteRuntimeBackend(_carrier_runtime_db_path(args.db))
    process = await backend.get_process(
        run_id=args.run_id,
        process_id=args.process_id,
    )
    if process is None:
        return {
            "ok": False,
            "run_id": args.run_id,
            "process_id": args.process_id,
            "error": "process not found",
        }

    deterministic = bool(process.metadata.get("deterministic"))
    base = {
        "ok": True,
        "run_id": process.run_id,
        "process_id": process.id,
        "status": process.status.value,
        "deterministic": deterministic,
        "recorded": {
            "input": process.input,
            "output": process.output,
            "error": process.error,
        },
    }
    if not args.rerun:
        return {
            **base,
            "mode": "recorded",
            "rerunnable": deterministic,
        }
    if not deterministic:
        return {
            **base,
            "ok": False,
            "mode": "rerun",
            "error": "process is not marked deterministic",
        }
    if process.status != CarrierProcessStatus.succeeded:
        return {
            **base,
            "ok": False,
            "mode": "rerun",
            "error": f"process is not succeeded: {process.status.value}",
        }

    adapter, step_input, config = _process_step_request_parts(process)
    if adapter.kind in {"manual_gate", "fala_runtime"}:
        return {
            **base,
            "ok": False,
            "mode": "rerun",
            "error": f"{adapter.kind} processes cannot be execution-rerun locally",
        }
    work_dir = Path(args.work_dir).expanduser() if args.work_dir else None
    if work_dir is not None:
        work_dir.mkdir(parents=True, exist_ok=True)
    result = await create_step_adapter(adapter.kind).run(
        StepRunRequest(
            run_id=process.run_id,
            process_id=process.id,
            carrier_id=process.carrier_id,
            adapter=adapter,
            input=step_input,
            config=config,
            work_dir=work_dir,
        )
    )
    rerun_output = {
        **result.output,
        "adapter": {
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        },
    }
    return {
        **base,
        "mode": "rerun",
        "rerun": {
            "output": rerun_output,
            "matches_recorded_output": rerun_output == process.output,
        },
    }


async def _carrier_runtime_enqueue_fala_runtime_process(
    *,
    backend: SQLiteRuntimeBackend,
    service: RuntimeBackendService,
    process: CarrierProcess,
    request: StepRunRequest,
    db_path: str,
    actor: str,
) -> StepRunResult:
    if request.adapter.runtime_ref is None:
        raise ValueError("fala_runtime adapter requires runtime_ref")
    if process.carrier_id is None:
        raise ValueError("fala_runtime process requires carrier_id")

    carrier = await backend.get_carrier(
        run_id=process.run_id,
        carrier_id=process.carrier_id,
    )
    if carrier is None:
        raise ValueError(f"Unknown carrier for fala_runtime process: {process.carrier_id!r}")

    events = await backend.list_events(
        run_id=process.run_id,
        carrier_id=process.carrier_id,
    )
    source_runtime = RuntimeRef(
        id=str(request.config.get("source_runtime_id") or "local"),
        uri=f"sqlite://{Path(db_path).expanduser().resolve()}",
    )
    target_runtime, pool_id, budget = await _resolve_fala_runtime_target(
        backend=backend,
        carrier=carrier,
        request=request,
    )
    target_run_id = str(request.config.get("target_run_id") or process.run_id)
    delivery_id = str(
        request.config.get("delivery_id")
        or f"bridge:{process.run_id}:{process.id}"
    )
    delivery = BridgeDelivery(
        id=delivery_id,
        run_id=process.run_id,
        idempotency_key=f"{process.run_id}:bridge.enqueue:{process.id}:{process.attempt}",
        source=RunRef(runtime=source_runtime, run_id=process.run_id),
        target=RunRef(runtime=target_runtime, run_id=target_run_id),
        carrier=carrier,
        event_ref=EventRef(
            runtime=source_runtime,
            run_id=process.run_id,
            event_id=events[-1].id if events else None,
            sequence=events[-1].sequence if events else None,
        ),
        pool_id=pool_id,
        budget=budget,
        metadata={
            "process_id": process.id,
            "process_type": process.process_type,
        },
    )
    outbox, submission = await service.enqueue_bridge_delivery(
        delivery,
        actor=actor,
    )
    return StepRunResult(
        waiting=True,
        output={
            "status": "submitted",
            "runtime_ref": request.adapter.runtime_ref,
            "target_run_id": target_run_id,
            "delivery_id": outbox.id,
            "command_id": submission.command.id,
            "replayed": submission.replayed,
        },
    )


async def _resolve_fala_runtime_target(
    *,
    backend: SQLiteRuntimeBackend,
    carrier: Carrier,
    request: StepRunRequest,
) -> tuple[RuntimeRef, str | None, RuntimeBudget]:
    assert request.adapter.runtime_ref is not None
    configured_budget = request.config.get("budget")
    pool = await backend.get_runtime_pool(pool_id=request.adapter.runtime_ref)
    if pool is None:
        return (
            RuntimeRef(
                id=str(
                    request.config.get("target_runtime_id")
                    or _runtime_ref_id(request.adapter.runtime_ref)
                ),
                uri=request.adapter.runtime_ref,
            ),
            request.config.get("pool_id"),
            RuntimeBudget.model_validate(configured_budget or {}),
        )

    if pool.carrier_types and carrier.carrier_type not in pool.carrier_types:
        raise ValueError(
            f"Runtime pool {pool.id!r} does not accept carrier type {carrier.carrier_type!r}"
        )
    if not pool.runtimes:
        raise ValueError(f"Runtime pool {pool.id!r} has no runtimes")

    policies = await backend.list_delegation_policies(pool_id=pool.id)
    delegation_policy = next(
        (
            item
            for item in policies
            if not item.carrier_types or carrier.carrier_type in item.carrier_types
        ),
        None,
    )
    budget = RuntimeBudget.model_validate(
        configured_budget
        or (
            delegation_policy.budget.model_dump(mode="json")
            if delegation_policy is not None
            else {}
        )
    )
    pool_policy = str(request.config.get("pool_policy") or pool.metadata.get("policy") or "manual")
    return await _select_runtime_from_pool(backend, pool=pool, policy=pool_policy), pool.id, budget


async def _select_runtime_from_pool(
    backend: SQLiteRuntimeBackend,
    *,
    pool: RuntimePool,
    policy: str,
) -> RuntimeRef:
    if policy in {"manual", "first"}:
        return pool.runtimes[0]
    if policy == "least_busy":
        return min(pool.runtimes, key=_runtime_declared_load)
    if policy == "round_robin":
        index = _int_metadata(pool.metadata.get("round_robin_index"))
        selected = pool.runtimes[index % len(pool.runtimes)]
        metadata = {
            **pool.metadata,
            "round_robin_index": (index + 1) % len(pool.runtimes),
        }
        await backend.put_runtime_pool(pool.model_copy(update={"metadata": metadata}))
        return selected
    raise ValueError(f"Unknown runtime pool policy: {policy!r}")


def _runtime_declared_load(runtime: RuntimeRef) -> float:
    value = runtime.metadata.get("load", runtime.metadata.get("pending_processes", 0))
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Runtime {runtime.id!r} has invalid load metadata") from exc


def _int_metadata(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError) as exc:
        raise ValueError("runtime pool round_robin_index metadata must be an integer") from exc


def _runtime_ref_id(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme in {"sqlite", "sqlite3"}:
        path = Path(_carrier_runtime_db_path(value))
        return path.stem or "sqlite"
    return value


def _process_step_request_parts(
    process: CarrierProcess,
) -> tuple[CarrierAdapterSpec, dict[str, Any], dict[str, Any]]:
    raw_input = dict(process.input)
    raw_adapter = raw_input.pop("adapter", None)
    if not isinstance(raw_adapter, dict):
        raise ValueError(f"Process {process.id!r} input requires adapter object")
    raw_config = raw_input.pop("config", {})
    config = raw_config if isinstance(raw_config, dict) else {}
    return CarrierAdapterSpec.model_validate(raw_adapter), raw_input, dict(config)


async def _carrier_runtime_gc(args: argparse.Namespace) -> dict[str, Any]:
    backend = SQLiteRuntimeBackend(_carrier_runtime_db_path(args.db))
    store = FileArtifactStore(args.artifact_root)
    cutoff = (
        time.time() - _parse_duration_seconds(args.older_than)
        if args.older_than
        else None
    )
    referenced: set[str] = set()
    all_run_ids = [run.id for run in await backend.list_runs()]
    run_ids = [args.run_id] if args.run_id else all_run_ids
    for run_id in all_run_ids:
        for artifact in await backend.list_artifacts(run_id=run_id):
            digest = digest_from_fala_artifact_uri(artifact.uri)
            if digest is not None:
                referenced.add(digest)

    collectable: list[str] = []
    kept: list[str] = []
    for blob in store.list_blobs():
        if blob.digest in referenced:
            kept.append(blob.digest)
            continue
        if cutoff is not None and Path(blob.location).stat().st_mtime >= cutoff:
            kept.append(blob.digest)
            continue
        collectable.append(blob.digest)

    deleted = [] if args.dry_run else store.delete_blobs(collectable)
    return {
        "ok": True,
        "dry_run": bool(args.dry_run),
        "artifact_root": str(store.root),
        "run_ids": run_ids,
        "scanned_run_ids": all_run_ids,
        "referenced_count": len(referenced),
        "kept_count": len(kept),
        "collectable_count": len(collectable),
        "deleted_count": len(deleted),
        "collectable": collectable,
        "deleted": deleted,
    }


def _parse_duration_seconds(value: str) -> float:
    suffixes = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    unit = value[-1]
    if unit in suffixes:
        number = value[:-1]
        multiplier = suffixes[unit]
    else:
        number = value
        multiplier = 1
    try:
        seconds = float(number) * multiplier
    except ValueError as exc:
        raise ValueError(f"Invalid duration: {value!r}") from exc
    if seconds < 0:
        raise ValueError("--older-than must be non-negative")
    return seconds


def _carrier_runtime_doctor(args: argparse.Namespace) -> dict[str, Any]:
    db_path = Path(_carrier_runtime_db_path(args.db))
    packages = _carrier_runtime_package_reports(getattr(args, "packages", []))
    packages_ok = all(package["ok"] for package in packages)
    if args.ensure_schema:
        SQLiteRuntimeBackend(db_path)
    if not db_path.exists():
        report = {
            "ok": False,
            "store_kind": "sqlite",
            "path": str(db_path),
            "error": f"SQLite database does not exist: {db_path}",
            "packages": packages,
        }
        return _write_carrier_runtime_doctor_report(args, report)

    with sqlite3.connect(db_path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        missing_tables = sorted(set(_CARRIER_RUNTIME_REQUIRED_TABLES) - tables)
        migration = None
        if "schema_migrations" in tables:
            migration = connection.execute(
                """
                SELECT version, applied_at
                FROM schema_migrations
                WHERE id = 'runtime_backend'
                """
            ).fetchone()
        counts = {
            table: _sqlite_count(connection, table) if table in tables else None
            for table in (
                "runs",
                "carriers",
                "runtime_commands",
                "runtime_events",
                "observations",
                "artifacts",
                "processes",
                "gates",
                "projections",
                "bridge_outbox",
                "bridge_inbox",
            )
        }

    latest_version = SQLITE_RUNTIME_SCHEMA_VERSION
    current_version = int(migration[0]) if migration is not None else None
    schema_ok = not missing_tables and current_version == latest_version
    report = {
        "ok": schema_ok and packages_ok,
        "store_kind": "sqlite",
        "path": str(db_path),
        "schema": {
            "required_tables": list(_CARRIER_RUNTIME_REQUIRED_TABLES),
            "missing_tables": missing_tables,
            "current_version": current_version,
            "latest_version": latest_version,
            "migrations": {
                "ok": current_version == latest_version,
                "applied_at": migration[1] if migration is not None else None,
                "missing": []
                if current_version == latest_version
                else ["runtime_backend"],
            },
        },
        "counts": counts,
        "packages": packages,
    }
    return _write_carrier_runtime_doctor_report(args, report)


def _carrier_runtime_package_reports(paths: list[str]) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    for raw_path in paths:
        path = Path(raw_path).expanduser()
        try:
            package = load_carrier_workflow_package_yaml(path)
        except Exception as exc:
            reports.append(
                {
                    "ok": False,
                    "path": str(path),
                    "error": str(exc),
                }
            )
        else:
            adapter_errors = _carrier_package_adapter_errors(package)
            reports.append(
                {
                    "ok": not adapter_errors,
                    "path": str(path),
                    "id": package.id,
                    "version": package.version,
                    "carrier_type_count": len(package.carrier_types),
                    "flow_count": len(package.flows),
                    "adapter_errors": adapter_errors,
                }
            )
    return reports


def _carrier_package_adapter_errors(
    package: CarrierWorkflowPackageSpec,
) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    for flow in package.flows:
        for step in flow.steps:
            adapter = step.adapter
            label = f"{flow.id}.{step.id}"
            try:
                if adapter.kind == "python_function" and adapter.ref:
                    _resolve_python_ref(adapter.ref)
                if adapter.kind == "subprocess":
                    _validate_subprocess_adapter_reference(adapter)
            except Exception as exc:
                errors.append(
                    {
                        "step": label,
                        "adapter_kind": adapter.kind,
                        "error": str(exc),
                    }
                )
    return errors


def _resolve_python_ref(ref: str) -> Any:
    module_name, separator, attr_name = ref.partition(":")
    if not separator:
        module_name, separator, attr_name = ref.rpartition(".")
    if not module_name or not attr_name:
        raise ValueError(f"invalid python ref: {ref!r}")
    return getattr(import_module(module_name), attr_name)


def _validate_subprocess_adapter_reference(adapter: CarrierAdapterSpec) -> None:
    cwd = Path(adapter.cwd).expanduser() if adapter.cwd else Path.cwd()
    if not cwd.exists() or not cwd.is_dir():
        raise ValueError(f"subprocess cwd does not exist: {cwd}")
    command = adapter.command or []
    if len(command) >= 2 and Path(command[1]).suffix == ".py":
        script = Path(command[1])
        if not script.is_absolute():
            script = cwd / script
        if not script.exists() or not script.is_file():
            raise ValueError(f"subprocess script does not exist: {script}")


def _write_carrier_runtime_doctor_report(
    args: argparse.Namespace,
    report: dict[str, Any],
) -> dict[str, Any]:
    if not args.output:
        return report
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "ok": report["ok"],
        "output": str(output),
        "store_kind": report["store_kind"],
        "missing_table_count": len(report.get("schema", {}).get("missing_tables", [])),
        "current_version": report.get("schema", {}).get("current_version"),
        "latest_version": report.get("schema", {}).get("latest_version"),
        "package_count": len(report.get("packages", [])),
        "package_error_count": sum(
            1 for package in report.get("packages", []) if not package.get("ok")
        ),
    }


def _add_carrier_runtime_db_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--db", required=True, help="Runtime SQLite DB path or sqlite:// URL.")


def _add_carrier_runtime_db_run_args(parser: argparse.ArgumentParser) -> None:
    _add_carrier_runtime_db_arg(parser)
    parser.add_argument("--run-id", required=True)


def _sqlite_count(connection: sqlite3.Connection, table: str) -> int:
    return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _carrier_runtime_vacuum(db_path: str) -> dict[str, Any]:
    with sqlite3.connect(db_path) as connection:
        before = {
            "page_count": int(connection.execute("PRAGMA page_count").fetchone()[0]),
            "freelist_count": int(connection.execute("PRAGMA freelist_count").fetchone()[0]),
        }
        connection.execute("VACUUM")
        after = {
            "page_count": int(connection.execute("PRAGMA page_count").fetchone()[0]),
            "freelist_count": int(connection.execute("PRAGMA freelist_count").fetchone()[0]),
        }
    return {
        "ok": True,
        "path": str(db_path),
        "before": before,
        "after": after,
    }


async def _carrier_runtime_trace(args: argparse.Namespace) -> dict[str, Any]:
    backend = SQLiteRuntimeBackend(_carrier_runtime_db_path(args.db))
    run = await backend.get_run(run_id=args.run_id)
    events = await backend.list_events(run_id=args.run_id)
    carriers = await backend.list_carriers(run_id=args.run_id)
    carrier_relations = await backend.list_carrier_relations(run_id=args.run_id)
    observations = await backend.list_observations(run_id=args.run_id)
    artifacts = await backend.list_artifacts(run_id=args.run_id)
    processes = await backend.list_processes(run_id=args.run_id)
    gates = await backend.list_gates(run_id=args.run_id)
    projections = await backend.list_projections(run_id=args.run_id)
    trace = {
        "run_id": args.run_id,
        "run": run.model_dump(mode="json") if run is not None else None,
        "counts": {
            "artifacts": len(artifacts),
            "carrier_relations": len(carrier_relations),
            "carriers": len(carriers),
            "events": len(events),
            "gates": len(gates),
            "observations": len(observations),
            "processes": len(processes),
            "projections": len(projections),
        },
        "timeline": [
            {
                "sequence": event.sequence,
                "type": event.event_type,
                "carrier_id": event.carrier_id,
                "process_id": event.process_id,
                "actor": event.actor,
                "created_at": event.created_at.isoformat(),
            }
            for event in events
        ],
        "events": [event.model_dump(mode="json") for event in events],
        "carriers": [carrier.model_dump(mode="json") for carrier in carriers],
        "carrier_relations": [
            relation.model_dump(mode="json") for relation in carrier_relations
        ],
        "observations": [
            observation.model_dump(mode="json") for observation in observations
        ],
        "artifacts": [artifact.model_dump(mode="json") for artifact in artifacts],
        "processes": [process.model_dump(mode="json") for process in processes],
        "gates": [gate.model_dump(mode="json") for gate in gates],
        "projections": [
            projection.model_dump(mode="json") for projection in projections
        ],
    }
    return {"ok": True, "trace": trace}


async def _carrier_runtime_export_html(args: argparse.Namespace) -> dict[str, Any]:
    result = await _carrier_runtime_trace(args)
    trace = result["trace"]
    out = Path(args.out).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(_render_carrier_runtime_html(trace), encoding="utf-8")
    return {"ok": True, "run_id": args.run_id, "out": str(out)}


async def _carrier_runtime_export_bundle(args: argparse.Namespace) -> dict[str, Any]:
    result = await _carrier_runtime_trace(args)
    trace = result["trace"]
    out = Path(args.out).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    files = {
        "trace.json": json.dumps(trace, indent=2, sort_keys=True),
        "timeline.json": json.dumps(trace["timeline"], indent=2, sort_keys=True),
        "graph.dot": _render_carrier_runtime_dot(trace),
        "report.html": _render_carrier_runtime_html(trace),
    }
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        for name, content in files.items():
            bundle.writestr(name, content)
    return {
        "ok": True,
        "run_id": args.run_id,
        "out": str(out),
        "files": sorted(files),
    }


async def _carrier_runtime_archive_run(args: argparse.Namespace) -> dict[str, Any]:
    trace_args = argparse.Namespace(db=args.db, run_id=args.run_id)
    result = await _carrier_runtime_trace(trace_args)
    trace = result["trace"]
    if trace["run"] is None:
        return {"ok": False, "run_id": args.run_id, "error": "run not found"}
    if args.retention_days is not None and args.retention_days < 0:
        raise ValueError("--retention-days must be non-negative")

    out = Path(args.out).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    archived_at = time.time()
    archive = {
        "run_id": args.run_id,
        "schema_version": SQLITE_RUNTIME_SCHEMA_VERSION,
        "archived_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(archived_at)),
        "format": "fala-run-archive-v1",
    }
    retention = None
    if args.retention_days is not None:
        retain_until = archived_at + args.retention_days * 86400
        retention = {
            "retention_days": args.retention_days,
            "retain_until": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(retain_until)),
        }
        archive["retention"] = retention
    files = {
        "archive.json": json.dumps(archive, indent=2, sort_keys=True),
        "trace.json": json.dumps(trace, indent=2, sort_keys=True),
        "timeline.json": json.dumps(trace["timeline"], indent=2, sort_keys=True),
        "graph.dot": _render_carrier_runtime_dot(trace),
        "report.html": _render_carrier_runtime_html(trace),
    }
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        for name, content in files.items():
            bundle.writestr(name, content)
    return {
        "ok": True,
        "run_id": args.run_id,
        "out": str(out),
        "files": sorted(files),
        "retention": retention,
    }


def _carrier_runtime_archive_gc(args: argparse.Namespace) -> dict[str, Any]:
    root = Path(args.archive_root).expanduser().resolve()
    if not root.exists() or not root.is_dir():
        raise ValueError(f"Archive root does not exist: {root}")
    now = datetime.now(timezone.utc)
    expired: list[str] = []
    kept: list[str] = []
    invalid: list[dict[str, str]] = []
    for path in sorted(root.rglob("*.zip")):
        try:
            archive = _read_archive_manifest(path)
            retain_until = archive.get("retention", {}).get("retain_until")
            if not retain_until:
                kept.append(str(path))
                continue
            if _parse_utc_timestamp(str(retain_until)) <= now:
                expired.append(str(path))
            else:
                kept.append(str(path))
        except Exception as exc:
            invalid.append({"path": str(path), "error": str(exc)})

    deleted: list[str] = []
    if not args.dry_run:
        for raw_path in expired:
            path = Path(raw_path)
            path.unlink()
            deleted.append(raw_path)
    return {
        "ok": True,
        "archive_root": str(root),
        "dry_run": bool(args.dry_run),
        "expired_count": len(expired),
        "deleted_count": len(deleted),
        "kept_count": len(kept),
        "invalid_count": len(invalid),
        "expired": expired,
        "deleted": deleted,
        "kept": kept,
        "invalid": invalid,
    }


def _read_archive_manifest(path: Path) -> dict[str, Any]:
    with zipfile.ZipFile(path) as archive:
        loaded = json.loads(archive.read("archive.json"))
    if not isinstance(loaded, dict):
        raise ValueError("archive.json must contain an object")
    if loaded.get("format") != "fala-run-archive-v1":
        raise ValueError("not a Fala run archive")
    return loaded


def _parse_utc_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise ValueError(f"invalid UTC timestamp: {value!r}") from exc
    return parsed.replace(tzinfo=timezone.utc)


def _render_carrier_runtime_html(trace: dict[str, Any]) -> str:
    counts = trace["counts"]
    count_items = "\n".join(
        f"<li><span>{_html(key)}</span><strong>{_html(value)}</strong></li>"
        for key, value in sorted(counts.items())
    )
    event_rows = "\n".join(
        "<tr>"
        f"<td>{_html(item['sequence'])}</td>"
        f"<td>{_html(item['type'])}</td>"
        f"<td>{_html(item['carrier_id'] or '')}</td>"
        f"<td>{_html(item['process_id'] or '')}</td>"
        f"<td>{_html(item['actor'] or '')}</td>"
        f"<td>{_html(item['created_at'])}</td>"
        "</tr>"
        for item in trace["timeline"]
    )
    if not event_rows:
        event_rows = '<tr><td colspan="6">No events recorded.</td></tr>'
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Fala Carrier Runtime Report - {_html(trace["run_id"])}</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 32px; color: #172026; }}
    h1, h2 {{ margin: 0 0 12px; }}
    section {{ margin-top: 28px; }}
    ul.counts {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 8px; padding: 0; }}
    ul.counts li {{ list-style: none; border: 1px solid #d8dee4; padding: 10px; }}
    ul.counts span {{ display: block; color: #57606a; font-size: 12px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #d8dee4; padding: 8px; text-align: left; vertical-align: top; }}
    th {{ background: #f6f8fa; }}
    pre {{ background: #f6f8fa; border: 1px solid #d8dee4; overflow: auto; padding: 12px; }}
  </style>
</head>
<body>
  <h1>Fala Carrier Runtime Report</h1>
  <p>Run: <strong>{_html(trace["run_id"])}</strong></p>

  <section>
    <h2>Counts</h2>
    <ul class="counts">
      {count_items}
    </ul>
  </section>

  <section>
    <h2>Timeline</h2>
    <table>
      <thead>
        <tr><th>Seq</th><th>Type</th><th>Carrier</th><th>Process</th><th>Actor</th><th>Created</th></tr>
      </thead>
      <tbody>
        {event_rows}
      </tbody>
    </table>
  </section>

  <section>
    <h2>Trace JSON</h2>
    <pre>{_json_html(trace)}</pre>
  </section>
</body>
</html>
"""


def _html(value: Any) -> str:
    return html_escape(str(value), quote=True)


def _json_html(value: Any) -> str:
    return html_escape(json.dumps(value, indent=2, sort_keys=True), quote=True)


def _render_carrier_runtime_dot(trace: dict[str, Any]) -> str:
    lines = ["digraph fala_runtime {", "  rankdir=LR;"]
    for carrier in trace["carriers"]:
        label = f"{carrier['id']}\\n{carrier['carrier_type']}"
        lines.append(f"  {_dot_quote(carrier['id'])} [label={_dot_quote(label)}];")
    for relation in trace["carrier_relations"]:
        lines.append(
            "  "
            f"{_dot_quote(relation['source_carrier_id'])} -> "
            f"{_dot_quote(relation['target_carrier_id'])} "
            f"[label={_dot_quote(relation['relation_type'])}];"
        )
    lines.append("}")
    return "\n".join(lines) + "\n"


def _dot_quote(value: Any) -> str:
    return json.dumps(str(value))


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


def _parse_values(items: list[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    for item in items:
        key, sep, value = item.partition("=")
        if not sep or not key:
            raise ValueError(f"Invalid value {item!r}; expected key=value")
        values[key] = value
    return values


def _parse_json_object(value: str, label: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} must be a JSON object") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{label} must be a JSON object")
    return parsed


if __name__ == "__main__":
    raise SystemExit(main())
