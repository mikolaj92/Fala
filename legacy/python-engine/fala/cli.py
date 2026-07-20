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

from pydantic import BaseModel

from fala.adapters import EffectorRunRequest, create_effector_adapter
from fala.reactions import FileReactionStore, digest_from_fala_reaction_uri
from fala.driver import process_effector_request_parts as _process_effector_request_parts
from fala.driver import run_until_idle
from fala.driver import sqlite_db_path as _runtime_db_path
from fala.models import (
    ReactionKindSpec,
    ReactionRef,
    EffectorAdapterSpec,
    ReactionStoreConfig,
    CapabilitySpec,
    CorrelationPathSpec,
    EffectorSpec,
    ImpulseRelationSpec,
    RuntimeBackendConfig,
    RuntimeConfigSpec,
    ImpulseTypeSpec,
    FalaPackageSpec,
    AssociationKindSpec,
)
from fala.runtime_backend import Reaction
from fala.runtime_backend import BridgeDelivery
from fala.runtime_backend import BridgeDeliveryStatus
from fala.runtime_backend import Impulse
from fala.runtime_backend import ProcessStatus
from fala.runtime_backend import ImpulseRelation
from fala.runtime_backend import RunStatus
from fala.runtime_backend import ImpulseType
from fala.runtime_backend import CommandSubmission
from fala.runtime_backend import EventRef
from fala.runtime_backend import Homeostat
from fala.runtime_backend import HomeostatStatus
from fala.runtime_backend import Association
from fala.runtime_backend import Process
from fala.runtime_backend import Projection
from fala.runtime_backend import Run
from fala.runtime_backend import RunRef
from fala.runtime_backend import RuntimeBackendService
from fala.runtime_backend import RuntimeReactionBlob
from fala.runtime_backend import RuntimeReactionStore
from fala.runtime_backend import RuntimeBudget
from fala.runtime_backend import RuntimeCommand
from fala.runtime_backend import RuntimeEvent
from fala.runtime_backend import RuntimeRef
from fala.runtime_backend import SQLITE_RUNTIME_SCHEMA_VERSION
from fala.runtime_backend import Correlator
from fala.yaml_loader import load_fala_package_yaml

CONTRACT_MODELS: dict[str, type[BaseModel]] = {
    "adapter": EffectorAdapterSpec,
    "reaction": Reaction,
    "reaction-kind": ReactionKindSpec,
    "reaction-ref": ReactionRef,
    "impulse": Impulse,
    "impulse-reaction-store-config": ReactionStoreConfig,
    "impulse-capability": CapabilitySpec,
    "fala-package": FalaPackageSpec,
    "impulse-correlation-path": CorrelationPathSpec,
    "impulse-correlation-path-effector": EffectorSpec,
    "impulse-relation": ImpulseRelation,
    "impulse-relation-spec": ImpulseRelationSpec,
    "runtime-backend-config": RuntimeBackendConfig,
    "runtime-config": RuntimeConfigSpec,
    "impulse-type": ImpulseType,
    "impulse-type-spec": ImpulseTypeSpec,
    "command": RuntimeCommand,
    "command-submission": CommandSubmission,
    "event": RuntimeEvent,
    "event-ref": EventRef,
    "homeostat": Homeostat,
    "association": Association,
    "association-kind": AssociationKindSpec,
    "process": Process,
    "projection": Projection,
    "run": Run,
    "run-ref": RunRef,
    "runtime-budget": RuntimeBudget,
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
            "impulse-relations",
            "impulse-types",
            "impulses",
            "bridge",
            "doctor",
            "events",
            "export-bundle",
            "export-html",
            "homeostat",
            "init",
            "homeostats",
            "gc",
            "maintain-journal",
            "associations",
            "processes",
            "projections",
            "runs",
            "run-until-idle",
            "replay-execution",
            "diagnose-waits",
            "trace",
        }
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fala")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init", help="Initialize a local Impulse runtime workspace.")
    _add_runtime_db_arg(init, required=False, default=".fala/state.sqlite")
    init.add_argument("--reaction-root", default=".fala/reactions", help="Filesystem reaction store root.")

    schema = subparsers.add_parser("schema", help="Emit JSON Schema for an Impulse runtime contract.")
    schema.add_argument("model", choices=sorted(CONTRACT_MODELS))

    db = subparsers.add_parser(
        "db",
        help="Initialize, migrate, and inspect Impulse runtime SQLite databases.",
    )
    db_subparsers = db.add_subparsers(dest="db_command", required=True)
    db_init = db_subparsers.add_parser("init", help="Create the Impulse SQLite schema.")
    _add_runtime_db_arg(db_init)
    db_migrate = db_subparsers.add_parser("migrate", help="Apply pending Impulse SQLite migrations.")
    _add_runtime_db_arg(db_migrate)
    db_status = db_subparsers.add_parser("status", help="Report Impulse SQLite schema status.")
    _add_runtime_db_arg(db_status)
    db_status.add_argument("--ensure-schema", action="store_true")
    db_vacuum = db_subparsers.add_parser("vacuum", help="Compact the Impulse SQLite database.")
    _add_runtime_db_arg(db_vacuum)

    gc = subparsers.add_parser("gc", help="Garbage-collect unreferenced filesystem reaction blobs.")
    _add_runtime_db_arg(gc)
    gc.add_argument("--reaction-root", default=".fala/reactions", help="Filesystem reaction store root.")
    gc.add_argument("--run-id", default=None)
    gc.add_argument("--older-than", default=None, help="Only collect blobs older than duration like 30d, 12h, 20m.")
    gc.add_argument("--dry-run", action="store_true")

    maintain_journal = subparsers.add_parser("maintain-journal", help="Run retention, reaction GC, and optional VACUUM in one operation.")
    _add_runtime_db_arg(maintain_journal)
    maintain_journal.add_argument("--reaction-root", default=".fala/reactions", help="Filesystem reaction store root.")
    maintain_journal.add_argument("--older-than-days", type=float, required=True)
    maintain_journal.add_argument("--keep-last", type=int, default=None)
    maintain_journal.add_argument("--no-vacuum", action="store_true")
    maintain_journal.add_argument("--delete", action="store_true", help="Apply deletions. Defaults to dry-run.")

    archive_run = subparsers.add_parser("archive-run", help="Write a portable run archive bundle.")
    archive_run.add_argument("run_id")
    _add_runtime_db_arg(archive_run)
    archive_run.add_argument("--out", required=True, help="Output .zip path.")
    archive_run.add_argument("--retention-days", type=int, default=None, help="Record archive retention period in archive metadata.")

    archive_gc = subparsers.add_parser("archive-gc", help="Delete expired Fala run archive bundles.")
    archive_gc.add_argument("--archive-root", required=True, help="Directory containing .zip run archives.")
    archive_gc.add_argument("--dry-run", action="store_true")

    doctor = subparsers.add_parser("doctor", help="Check Impulse runtime readiness.")
    _add_runtime_db_arg(doctor, required=False, default=".fala/state.sqlite")
    doctor.add_argument("--ensure-schema", action="store_true", help="Create/repair Impulse runtime schema before checking.")
    doctor.add_argument("--package", dest="packages", action="append", default=[], help="Fala package YAML path to validate. Repeatable.")
    doctor.add_argument("--output", default=None, help="Write JSON doctor report to this path instead of stdout envelope.")

    create_run = subparsers.add_parser("create-run", help="Create an Impulse run.")
    _add_runtime_db_arg(create_run)
    create_run.add_argument("--run-id", default=None)
    create_run.add_argument("--title", default=None)
    create_run.add_argument("--package-id", default=None)
    create_run.add_argument("--package-version", default=None)
    create_run.add_argument("--package-digest", default=None)
    create_run.add_argument("--correlation_path-id", default=None)
    create_run.add_argument("--correlation_path-digest", default=None)
    create_run.add_argument("--runtime-version", default=None)
    create_run.add_argument("--backend-version", default=None)
    create_run.add_argument("--metadata", action="append", default=[])
    create_run.add_argument("--idempotency-key", default=None)

    runs = subparsers.add_parser("runs", help="Inspect Impulse runtime runs.")
    run_subparsers = runs.add_subparsers(dest="run_command", required=True)
    runs_list = run_subparsers.add_parser("list", help="List Impulse runs.")
    _add_runtime_db_arg(runs_list)
    runs_list.add_argument("--status", choices=[status.value for status in RunStatus], default=None)
    runs_list.add_argument("--limit", type=int, default=None)
    runs_list.add_argument("--jsonl", action="store_true")
    runs_inspect = run_subparsers.add_parser("inspect", help="Inspect one Impulse run.")
    _add_runtime_db_arg(runs_inspect)
    runs_inspect.add_argument("--run-id", required=True)
    runs_observe = run_subparsers.add_parser(
        "observe",
        help="Observe one run's boundary: derived status, process counts, event watermark.",
    )
    _add_runtime_db_arg(runs_observe)
    runs_observe.add_argument("--run-id", required=True)
    runs_cancel = run_subparsers.add_parser("cancel", help="Request cancellation for one Impulse run.")
    _add_runtime_db_arg(runs_cancel)
    runs_cancel.add_argument("--run-id", required=True)
    runs_cancel.add_argument("--reason", default=None)
    runs_cancel.add_argument("--idempotency-key", default=None)

    commands = subparsers.add_parser("commands", help="Inspect runtime commands.")
    command_subparsers = commands.add_subparsers(
        dest="command_command",
        required=True,
    )
    commands_list = command_subparsers.add_parser("list", help="List runtime commands.")
    _add_runtime_db_run_args(commands_list)
    commands_list.add_argument("--command-type", default=None)
    commands_list.add_argument("--actor", default=None)
    commands_list.add_argument("--limit", type=int, default=None)
    commands_list.add_argument("--jsonl", action="store_true")
    commands_inspect = command_subparsers.add_parser(
        "inspect",
        help="Inspect one runtime command.",
    )
    _add_runtime_db_run_args(commands_inspect)
    commands_inspect.add_argument("--command-id", required=True)

    impulses = subparsers.add_parser("impulses", help="Inspect Impulse runtime impulses.")
    impulse_subparsers = impulses.add_subparsers(dest="impulse_command", required=True)
    impulses_create = impulse_subparsers.add_parser("create", help="Create an impulse.")
    _add_runtime_db_run_args(impulses_create)
    impulses_create.add_argument("--impulse-id", default=None)
    impulses_create.add_argument("--impulse-type", required=True)
    impulses_create.add_argument("--payload-json", default="{}")
    impulses_create.add_argument("--metadata-json", default="{}")
    impulses_create.add_argument("--idempotency-key", default=None)
    impulses_list = impulse_subparsers.add_parser("list", help="List impulses.")
    _add_runtime_db_run_args(impulses_list)
    impulses_list.add_argument("--impulse-type", default=None)
    impulses_list.add_argument("--limit", type=int, default=None)
    impulses_list.add_argument("--jsonl", action="store_true")
    impulses_inspect = impulse_subparsers.add_parser("inspect", help="Inspect one impulse.")
    _add_runtime_db_run_args(impulses_inspect)
    impulses_inspect.add_argument("--impulse-id", required=True)

    impulse_types = subparsers.add_parser("impulse-types", help="Inspect Impulse type metadata.")
    impulse_type_subparsers = impulse_types.add_subparsers(dest="impulse_type_command", required=True)
    impulse_types_list = impulse_type_subparsers.add_parser("list", help="List impulse types.")
    _add_runtime_db_run_args(impulse_types_list)
    impulse_types_list.add_argument("--jsonl", action="store_true")
    impulse_types_inspect = impulse_type_subparsers.add_parser("inspect", help="Inspect one impulse type.")
    _add_runtime_db_run_args(impulse_types_inspect)
    impulse_types_inspect.add_argument("--impulse-type-id", required=True)

    impulse_relations = subparsers.add_parser("impulse-relations", help="Inspect Impulse relations.")
    impulse_relation_subparsers = impulse_relations.add_subparsers(dest="impulse_relation_command", required=True)
    impulse_relations_list = impulse_relation_subparsers.add_parser("list", help="List impulse relations.")
    _add_runtime_db_run_args(impulse_relations_list)
    impulse_relations_list.add_argument("--impulse-id", default=None)
    impulse_relations_list.add_argument("--relation-type", default=None)
    impulse_relations_list.add_argument("--jsonl", action="store_true")
    impulse_relations_inspect = impulse_relation_subparsers.add_parser("inspect", help="Inspect one impulse relation.")
    _add_runtime_db_run_args(impulse_relations_inspect)
    impulse_relations_inspect.add_argument("--relation-id", required=True)

    reactions = subparsers.add_parser("reactions", help="Inspect Impulse reaction metadata.")
    reaction_subparsers = reactions.add_subparsers(dest="reaction_command", required=True)
    reactions_record = reaction_subparsers.add_parser("record", help="Record one filesystem reaction.")
    _add_runtime_db_run_args(reactions_record)
    reactions_record.add_argument("--reaction-root", default=".fala/reactions")
    reactions_record.add_argument("--path", required=True)
    reactions_record.add_argument("--kind", required=True)
    reactions_record.add_argument("--reaction-id", default=None)
    reactions_record.add_argument("--impulse-id", default=None)
    reactions_record.add_argument("--media-type", default=None)
    reactions_record.add_argument("--metadata-json", default="{}")
    reactions_record.add_argument("--idempotency-key", default=None)
    reactions_list = reaction_subparsers.add_parser("list", help="List reactions.")
    _add_runtime_db_run_args(reactions_list)
    reactions_list.add_argument("--impulse-id", default=None)
    reactions_list.add_argument("--kind", default=None)
    reactions_list.add_argument("--jsonl", action="store_true")
    reactions_inspect = reaction_subparsers.add_parser("inspect", help="Inspect one reaction.")
    _add_runtime_db_run_args(reactions_inspect)
    reactions_inspect.add_argument("--reaction-id", required=True)

    processes = subparsers.add_parser("processes", help="Inspect Impulse runtime processes.")
    process_subparsers = processes.add_subparsers(dest="process_command", required=True)
    processes_schedule = process_subparsers.add_parser("schedule", help="Schedule an impulse process.")
    _add_runtime_db_run_args(processes_schedule)
    processes_schedule.add_argument("--process-id", default=None)
    processes_schedule.add_argument("--impulse-id", default=None)
    processes_schedule.add_argument("--process-type", required=True)
    processes_schedule.add_argument("--status", choices=["pending", "ready"], default="ready")
    processes_schedule.add_argument("--priority", type=int, default=0)
    processes_schedule.add_argument("--max-attempts", type=int, default=1)
    processes_schedule.add_argument("--input-json", default="{}")
    processes_schedule.add_argument("--metadata-json", default="{}")
    processes_schedule.add_argument("--idempotency-key", default=None)
    processes_list = process_subparsers.add_parser("list", help="List processes.")
    _add_runtime_db_run_args(processes_list)
    processes_list.add_argument("--status", choices=[status.value for status in ProcessStatus], default=None)
    processes_list.add_argument("--impulse-id", default=None)
    processes_list.add_argument("--jsonl", action="store_true")
    processes_inspect = process_subparsers.add_parser("inspect", help="Inspect one process.")
    _add_runtime_db_run_args(processes_inspect)
    processes_inspect.add_argument("--process-id", required=True)
    processes_cancel = process_subparsers.add_parser("cancel", help="Cancel one process.")
    _add_runtime_db_run_args(processes_cancel)
    processes_cancel.add_argument("--process-id", required=True)
    processes_cancel.add_argument("--error-json", default="{}")
    processes_cancel.add_argument("--idempotency-key", default=None)
    processes_timeout = process_subparsers.add_parser("timeout", help="Mark one process timed out.")
    _add_runtime_db_run_args(processes_timeout)
    processes_timeout.add_argument("--process-id", required=True)
    processes_timeout.add_argument("--error-json", default="{}")
    processes_timeout.add_argument("--idempotency-key", default=None)

    associations = subparsers.add_parser("associations", help="Inspect Impulse associations.")
    association_subparsers = associations.add_subparsers(dest="association_command", required=True)
    associations_append = association_subparsers.add_parser("append", help="Append one association.")
    _add_runtime_db_run_args(associations_append)
    associations_append.add_argument("--association-id", default=None)
    associations_append.add_argument("--impulse-id", default=None)
    associations_append.add_argument("--kind", required=True)
    associations_append.add_argument("--values-json", default="{}")
    associations_append.add_argument("--metadata-json", default="{}")
    associations_append.add_argument("--idempotency-key", default=None)
    associations_list = association_subparsers.add_parser("list", help="List associations.")
    _add_runtime_db_run_args(associations_list)
    associations_list.add_argument("--impulse-id", default=None)
    associations_list.add_argument("--jsonl", action="store_true")
    associations_inspect = association_subparsers.add_parser("inspect", help="Inspect one association.")
    _add_runtime_db_run_args(associations_inspect)
    associations_inspect.add_argument("--association-id", required=True)

    events = subparsers.add_parser("events", help="Inspect Impulse runtime events.")
    event_subparsers = events.add_subparsers(dest="event_command", required=True)
    events_list = event_subparsers.add_parser("list", help="List ordered events.")
    _add_runtime_db_run_args(events_list)
    events_list.add_argument("--impulse-id", default=None)
    events_list.add_argument("--after-sequence", type=int, default=None)
    events_list.add_argument("--limit", type=int, default=None)
    events_list.add_argument("--jsonl", action="store_true")
    events_validate = event_subparsers.add_parser(
        "validate-schema",
        help="Validate event schema versions for a run.",
    )
    _add_runtime_db_run_args(events_validate)
    events_validate.add_argument("--max-schema-version", type=int, default=1)

    homeostats = subparsers.add_parser("homeostats", help="Inspect Impulse runtime homeostats.")
    homeostat_subparsers = homeostats.add_subparsers(dest="homeostat_command", required=True)
    homeostats_list = homeostat_subparsers.add_parser("list", help="List homeostats.")
    _add_runtime_db_run_args(homeostats_list)
    homeostats_list.add_argument("--impulse-id", default=None)
    homeostats_list.add_argument("--status", choices=[status.value for status in HomeostatStatus], default=None)
    homeostats_list.add_argument("--jsonl", action="store_true")

    homeostat = subparsers.add_parser("homeostat", help="Mutate one Impulse runtime homeostat.")
    single_homeostat_subparsers = homeostat.add_subparsers(dest="homeostat_command", required=True)
    homeostat_open = single_homeostat_subparsers.add_parser("open", help="Open a homeostat.")
    _add_runtime_db_run_args(homeostat_open)
    homeostat_open.add_argument("--homeostat-id", default=None)
    homeostat_open.add_argument("--impulse-id", default=None)
    homeostat_open.add_argument("--kind", required=True)
    homeostat_open.add_argument("--values-json", default="{}")
    homeostat_open.add_argument("--metadata-json", default="{}")
    homeostat_open.add_argument("--idempotency-key", default=None)
    homeostat_complete = single_homeostat_subparsers.add_parser("complete", help="Complete an open homeostat.")
    _add_runtime_db_run_args(homeostat_complete)
    homeostat_complete.add_argument("--homeostat-id", required=True)
    homeostat_complete.add_argument("--value", action="append", default=[], help="Homeostat output value as key=value. Repeatable.")
    homeostat_complete.add_argument("--idempotency-key", default=None)
    homeostat_cancel = single_homeostat_subparsers.add_parser("cancel", help="Cancel an open homeostat.")
    _add_runtime_db_run_args(homeostat_cancel)
    homeostat_cancel.add_argument("--homeostat-id", required=True)
    homeostat_cancel.add_argument("--value", action="append", default=[], help="Homeostat output value as key=value. Repeatable.")
    homeostat_cancel.add_argument("--idempotency-key", default=None)
    homeostat_expire = single_homeostat_subparsers.add_parser("expire", help="Expire an open homeostat.")
    _add_runtime_db_run_args(homeostat_expire)
    homeostat_expire.add_argument("--homeostat-id", required=True)
    homeostat_expire.add_argument("--value", action="append", default=[], help="Homeostat output value as key=value. Repeatable.")
    homeostat_expire.add_argument("--idempotency-key", default=None)

    projections = subparsers.add_parser("projections", help="Inspect Impulse projections.")
    projection_subparsers = projections.add_subparsers(dest="projection_command", required=True)
    projections_list = projection_subparsers.add_parser("list", help="List projections.")
    _add_runtime_db_run_args(projections_list)
    projections_list.add_argument("--jsonl", action="store_true")
    projections_rebuild = projection_subparsers.add_parser("rebuild", help="Rebuild Impulse projections.")
    _add_runtime_db_run_args(projections_rebuild)
    projections_rebuild.add_argument("--name", action="append", default=[], help="Projection name to rebuild. Repeatable. Defaults to all built-ins.")
    projections_rebuild.add_argument("--idempotency-key", default=None)
    projections_rebuild.add_argument("--jsonl", action="store_true")

    bridge = subparsers.add_parser("bridge", help="Inspect and deliver Impulse runtime bridge records.")
    bridge_subparsers = bridge.add_subparsers(dest="bridge_command", required=True)
    bridge_list = bridge_subparsers.add_parser("list", help="List bridge deliveries.")
    _add_runtime_db_run_args(bridge_list)
    bridge_list.add_argument("--box", choices=("outbox", "inbox"), default="outbox")
    bridge_list.add_argument("--status", choices=[status.value for status in BridgeDeliveryStatus], default=None)
    bridge_list.add_argument("--jsonl", action="store_true")
    bridge_deliver = bridge_subparsers.add_parser("deliver", help="Deliver one outbox record into another local SQLite runtime.")
    _add_runtime_db_run_args(bridge_deliver)
    bridge_deliver.add_argument("--delivery-id", required=True)
    bridge_deliver.add_argument("--target-db", required=True)
    bridge_deliver.add_argument("--idempotency-key", default=None)
    bridge_deliver.add_argument("--import-idempotency-key", default=None)
    bridge_export = bridge_subparsers.add_parser("export", help="Export one outbox bridge delivery to JSON.")
    _add_runtime_db_run_args(bridge_export)
    bridge_export.add_argument("--delivery-id", required=True)
    bridge_export.add_argument("--out", required=True)
    bridge_import = bridge_subparsers.add_parser("import", help="Import one bridge delivery JSON file.")
    _add_runtime_db_arg(bridge_import)
    bridge_import.add_argument("--file", required=True)
    bridge_import.add_argument("--idempotency-key", default=None)

    run_until_idle = subparsers.add_parser("run-until-idle", help="Run ready Impulse processes until idle.")
    _add_runtime_db_arg(run_until_idle)
    run_until_idle.add_argument("--run-id", default=None)
    run_until_idle.add_argument("--all-runs", action="store_true")
    run_until_idle.add_argument("--worker-id", default="cli:run-until-idle")
    run_until_idle.add_argument("--lease-seconds", type=float, default=300.0)
    run_until_idle.add_argument("--max-ticks", type=int, default=100)
    run_until_idle.add_argument("--work-dir", default=None)

    replay_execution = subparsers.add_parser("replay-execution", help="Replay or verify a recorded Impulse process execution.")
    _add_runtime_db_arg(replay_execution)
    replay_execution.add_argument("--run-id", required=True)
    replay_execution.add_argument("--process-id", required=True)
    replay_execution.add_argument("--rerun", action="store_true", help="Rerun only if process metadata marks it deterministic.")
    replay_execution.add_argument(
        "--compare",
        action="store_true",
        help="Rerun and report a structural path-by-path diff of rerun vs recorded output.",
    )
    replay_execution.add_argument("--work-dir", default=None)

    diagnose_waits = subparsers.add_parser("diagnose-waits", help="Diagnose Impulse waits and wait-graph deadlocks.")
    _add_runtime_db_arg(diagnose_waits)
    diagnose_waits.add_argument("--run-id", required=True)
    diagnose_waits.add_argument("--impulse-id", default=None)

    trace = subparsers.add_parser("trace", help="Show Impulse runtime trace for one run.")
    _add_runtime_db_arg(trace)
    trace.add_argument("--run-id", required=True)

    export_html = subparsers.add_parser("export-html", help="Export a static Impulse runtime HTML report.")
    _add_runtime_db_arg(export_html)
    export_html.add_argument("--run-id", required=True)
    export_html.add_argument("--out", required=True, help="Output HTML path.")

    export_bundle = subparsers.add_parser("export-bundle", help="Export a portable Impulse runtime debug bundle.")
    _add_runtime_db_arg(export_bundle)
    export_bundle.add_argument("--run-id", required=True)
    export_bundle.add_argument("--out", required=True, help="Output .zip path.")

    return parser

async def _run(args: argparse.Namespace) -> dict[str, Any] | None:
    if args.command == "init":
        db_path = Path(_cli_db_path(args))
        reaction_root = Path(args.reaction_root).expanduser()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        reaction_root.mkdir(parents=True, exist_ok=True)
        Correlator(db_path)
        return {
            "ok": True,
            "db": str(db_path),
            "reaction_root": str(reaction_root),
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
        db_path = _cli_db_path(args)
        if args.db_command in {"init", "migrate"}:
            Correlator(db_path)
            return {
                "ok": True,
                "path": str(db_path),
                "schema_version": SQLITE_RUNTIME_SCHEMA_VERSION,
            }
        if args.db_command == "vacuum":
            return _runtime_vacuum(db_path)
        return _runtime_doctor(
            argparse.Namespace(
                db=getattr(args, "db", None),
                journal=getattr(args, "journal", None) or getattr(args, "db", None),
                ensure_schema=args.ensure_schema,
                packages=[],
                output=None,
            )
        )

    if args.command == "doctor":
        return _runtime_doctor(args)

    if args.command in {
        "archive-gc",
        "archive-run",
        "reactions",
        "bridge",
        "create-run",
        "impulse-relations",
        "impulse-types",
        "impulses",
        "commands",
        "diagnose-waits",
        "events",
        "export-bundle",
        "export-html",
        "homeostat",
        "homeostats",
        "gc",
        "maintain-journal",
        "associations",
        "processes",
        "projections",
        "run-until-idle",
        "replay-execution",
        "runs",
        "trace",
    }:
        return await _runtime_command(args)

    raise ValueError(f"Unknown Fala command: {args.command}")

_RUNTIME_REQUIRED_TABLES = (
    "reactions",
    "bridge_inbox",
    "bridge_outbox",
    "impulse_relations",
    "impulse_types",
    "impulses",
    "delegation_policies",
    "homeostats",
    "associations",
    "processes",
    "projections",
    "runtime_pools",
    "runtime_commands",
    "runtime_events",
    "runs",
    "schema_migrations",
)


async def _runtime_command(args: argparse.Namespace) -> dict[str, Any] | None:
    if args.command == "diagnose-waits":
        service = RuntimeBackendService(
            Correlator(_cli_db_path(args))
        )
        diagnostic = await service.diagnose_waits(
            run_id=args.run_id,
            impulse_id=args.impulse_id,
        )
        return {
            "ok": True,
            "wait_diagnostics": diagnostic.model_dump(mode="json"),
        }
    if args.command == "trace":
        return await _runtime_trace(args)
    if args.command == "archive-gc":
        return _runtime_archive_gc(args)
    if args.command == "archive-run":
        return await _runtime_archive_run(args)
    if args.command == "export-html":
        return await _runtime_export_html(args)
    if args.command == "export-bundle":
        return await _runtime_export_bundle(args)
    if args.command == "gc":
        return await _runtime_gc(args)
    if args.command == "maintain-journal":
        return await _runtime_maintain_journal(args)
    if args.command == "run-until-idle":
        return await _runtime_run_until_idle(args)
    if args.command == "replay-execution":
        return await _runtime_replay_execution(args)

    backend = Correlator(_cli_db_path(args))
    if args.command == "create-run":
        run_data = {
            "title": args.title,
            "package_id": args.package_id,
            "package_version": args.package_version,
            "package_digest": args.package_digest,
            "correlation_path_id": args.correlation_path_id,
            "correlation_path_digest": args.correlation_path_digest,
            "runtime_version": args.runtime_version,
            "backend_version": args.backend_version,
            "metadata": _parse_values(args.metadata),
        }
        if args.run_id is not None:
            run_data["id"] = args.run_id
        run = Run.model_validate(run_data)
        service = RuntimeBackendService(backend)
        stored, submission = await service.create_run(
            run,
            idempotency_key=args.idempotency_key or "run.create",
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
                status=RunStatus(args.status) if args.status else None,
                limit=args.limit,
            )
            return _runtime_list_result("runs", runs, jsonl=args.jsonl)
        if args.run_command == "observe":
            service = RuntimeBackendService(backend)
            boundary = await service.observe_run(run_id=args.run_id)
            return {
                "ok": True,
                "boundary": boundary.model_dump(mode="json"),
            }
        if args.run_command == "cancel":
            service = RuntimeBackendService(backend)
            run, submission = await service.cancel_run(
                run_id=args.run_id,
                idempotency_key=args.idempotency_key or "run.cancel",
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
        return _runtime_list_result(
            "commands",
            commands,
            jsonl=args.jsonl,
        )
    if args.command == "impulses":
        if args.impulse_command == "create":
            impulse_data = {
                "run_id": args.run_id,
                "impulse_type": args.impulse_type,
                "payload": _parse_json_object(args.payload_json, "--payload-json"),
                "metadata": _parse_json_object(args.metadata_json, "--metadata-json"),
            }
            if args.impulse_id is not None:
                impulse_data["id"] = args.impulse_id
            impulse = Impulse.model_validate(impulse_data)
            service = RuntimeBackendService(backend)
            stored, submission = await service.accept_impulse(
                impulse,
                idempotency_key=args.idempotency_key
                or f"impulse.accept:{impulse.id}",
                actor="cli:user",
            )
            return {
                "ok": True,
                "impulse": stored.model_dump(mode="json"),
                "command": submission.command.model_dump(mode="json"),
                "replayed": submission.replayed,
            }
        if args.impulse_command == "list":
            impulses = await backend.list_impulses(
                run_id=args.run_id,
                impulse_type=args.impulse_type,
                limit=args.limit,
            )
            return _runtime_list_result("impulses", impulses, jsonl=args.jsonl)
        impulse = await backend.get_impulse(
            run_id=args.run_id,
            impulse_id=args.impulse_id,
        )
        return {
            "ok": impulse is not None,
            "impulse": impulse.model_dump(mode="json") if impulse is not None else None,
        }
    if args.command == "impulse-types":
        if args.impulse_type_command == "list":
            impulse_types = await backend.list_impulse_types(run_id=args.run_id)
            return _runtime_list_result(
                "impulse_types",
                impulse_types,
                jsonl=args.jsonl,
            )
        impulse_type = await backend.get_impulse_type(
            run_id=args.run_id,
            impulse_type_id=args.impulse_type_id,
        )
        return {
            "ok": impulse_type is not None,
            "impulse_type": impulse_type.model_dump(mode="json")
            if impulse_type is not None
            else None,
        }
    if args.command == "impulse-relations":
        if args.impulse_relation_command == "list":
            relations = await backend.list_impulse_relations(
                run_id=args.run_id,
                impulse_id=args.impulse_id,
                relation_type=args.relation_type,
            )
            return _runtime_list_result(
                "impulse_relations",
                relations,
                jsonl=args.jsonl,
            )
        relation = await backend.get_impulse_relation(
            run_id=args.run_id,
            relation_id=args.relation_id,
        )
        return {
            "ok": relation is not None,
            "impulse_relation": relation.model_dump(mode="json")
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
            return _runtime_list_result(
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
                or f"bridge.file.import:{delivery.id}",
                actor="cli:user",
            )
            return {
                "ok": True,
                "imported": imported.model_dump(mode="json"),
                "command": submission.command.model_dump(mode="json"),
                "replayed": submission.replayed,
            }
        target = RuntimeBackendService.sqlite(_runtime_db_path(args.target_db))
        delivered, imported, delivery_submission, import_submission = (
            await service.deliver_bridge_delivery(
                run_id=args.run_id,
                delivery_id=args.delivery_id,
                target=target,
                idempotency_key=args.idempotency_key
                or f"bridge.deliver:{args.delivery_id}",
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
    if args.command == "reactions":
        if args.reaction_command == "record":
            store = FileReactionStore(args.reaction_root)
            ref = store.put_file(
                kind=args.kind,
                path=args.path,
                reaction_id=args.reaction_id,
                metadata=_parse_json_object(args.metadata_json, "--metadata-json"),
            )
            digest = ref.metadata.get("sha256")
            reaction = Reaction(
                id=ref.id,
                run_id=args.run_id,
                impulse_id=args.impulse_id,
                kind=args.kind,
                uri=ref.uri,
                media_type=args.media_type,
                size_bytes=ref.metadata.get("size_bytes"),
                content_hash=f"sha256:{digest}" if isinstance(digest, str) else None,
                metadata={**ref.metadata, "reaction_store": store.location},
            )
            service = RuntimeBackendService(backend)
            stored, submission = await service.record_reaction(
                reaction,
                idempotency_key=args.idempotency_key
                or f"reaction.record:{reaction.id}",
                actor="cli:user",
            )
            return {
                "ok": True,
                "reaction": stored.model_dump(mode="json"),
                "command": submission.command.model_dump(mode="json"),
                "replayed": submission.replayed,
            }
        if args.reaction_command == "list":
            reactions = await backend.list_reactions(
                run_id=args.run_id,
                impulse_id=args.impulse_id,
                kind=args.kind,
            )
            return _runtime_list_result(
                "reactions",
                reactions,
                jsonl=args.jsonl,
            )
        reaction = await backend.get_reaction(
            run_id=args.run_id,
            reaction_id=args.reaction_id,
        )
        return {
            "ok": reaction is not None,
            "reaction": reaction.model_dump(mode="json")
            if reaction is not None
            else None,
        }
    if args.command == "processes":
        if args.process_command == "schedule":
            process_data = {
                "run_id": args.run_id,
                "impulse_id": args.impulse_id,
                "process_type": args.process_type,
                "status": ProcessStatus(args.status),
                "priority": args.priority,
                "max_attempts": args.max_attempts,
                "input": _parse_json_object(args.input_json, "--input-json"),
                "metadata": _parse_json_object(args.metadata_json, "--metadata-json"),
            }
            if args.process_id is not None:
                process_data["id"] = args.process_id
            process = Process.model_validate(process_data)
            service = RuntimeBackendService(backend)
            stored, submission = await service.schedule_process(
                process,
                idempotency_key=args.idempotency_key
                or f"process.schedule:{process.id}",
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
                or f"process.{args.process_command}:{args.process_id}",
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
                status=ProcessStatus(args.status) if args.status else None,
                impulse_id=args.impulse_id,
            )
            return _runtime_list_result(
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
    if args.command == "associations":
        if args.association_command == "append":
            association_data = {
                "run_id": args.run_id,
                "impulse_id": args.impulse_id,
                "kind": args.kind,
                "values": _parse_json_object(args.values_json, "--values-json"),
                "metadata": _parse_json_object(args.metadata_json, "--metadata-json"),
            }
            if args.association_id is not None:
                association_data["id"] = args.association_id
            association = Association.model_validate(association_data)
            service = RuntimeBackendService(backend)
            stored, submission = await service.record_association(
                association,
                idempotency_key=args.idempotency_key
                or f"association.record:{association.id}",
                actor="cli:user",
            )
            return {
                "ok": True,
                "association": stored.model_dump(mode="json"),
                "command": submission.command.model_dump(mode="json"),
                "replayed": submission.replayed,
            }
        if args.association_command == "inspect":
            associations = await backend.list_associations(run_id=args.run_id)
            association = next(
                (
                    item
                    for item in associations
                    if item.id == args.association_id
                ),
                None,
            )
            return {
                "ok": association is not None,
                "association": association.model_dump(mode="json")
                if association is not None
                else None,
            }
        associations = await backend.list_associations(
            run_id=args.run_id,
            impulse_id=args.impulse_id,
        )
        return _runtime_list_result(
            "associations",
            associations,
            jsonl=args.jsonl,
        )
    if args.command == "events":
        if args.event_command == "validate-schema":
            events = await backend.list_events(run_id=args.run_id)
            return _runtime_event_schema_report(
                events,
                max_schema_version=args.max_schema_version,
            )
        events = await backend.list_events(
            run_id=args.run_id,
            impulse_id=args.impulse_id,
            after_sequence=args.after_sequence,
            limit=args.limit,
        )
        return _runtime_list_result("events", events, jsonl=args.jsonl)
    if args.command in {"homeostat", "homeostats"}:
        if args.homeostat_command == "open":
            homeostat_data = {
                "run_id": args.run_id,
                "impulse_id": args.impulse_id,
                "kind": args.kind,
                "values": _parse_json_object(args.values_json, "--values-json"),
                "metadata": _parse_json_object(args.metadata_json, "--metadata-json"),
            }
            if args.homeostat_id is not None:
                homeostat_data["id"] = args.homeostat_id
            homeostat = Homeostat.model_validate(homeostat_data)
            service = RuntimeBackendService(backend)
            opened, submission = await service.open_homeostat(
                homeostat,
                idempotency_key=args.idempotency_key
                or f"homeostat.open:{homeostat.id}",
                actor="cli:user",
            )
            return {
                "ok": True,
                "homeostat": opened.model_dump(mode="json"),
                "command": submission.command.model_dump(mode="json"),
                "replayed": submission.replayed,
            }
        if args.homeostat_command in {"complete", "cancel", "expire"}:
            service = RuntimeBackendService(backend)
            method = {
                "complete": service.complete_homeostat,
                "cancel": service.cancel_homeostat,
                "expire": service.expire_homeostat,
            }[args.homeostat_command]
            homeostat, submission = await method(
                run_id=args.run_id,
                homeostat_id=args.homeostat_id,
                values=_parse_values(args.value),
                idempotency_key=args.idempotency_key
                or f"homeostat.{args.homeostat_command}:{args.homeostat_id}",
                actor="cli:user",
            )
            return {
                "ok": True,
                "homeostat": homeostat.model_dump(mode="json"),
                "command": submission.command.model_dump(mode="json"),
                "replayed": submission.replayed,
            }
        homeostats = await backend.list_homeostats(
            run_id=args.run_id,
            impulse_id=args.impulse_id,
            status=HomeostatStatus(args.status) if args.status else None,
        )
        return _runtime_list_result("homeostats", homeostats, jsonl=args.jsonl)
    if args.command == "projections":
        if args.projection_command == "rebuild":
            service = RuntimeBackendService(backend)
            names = args.name or None
            rebuilt, submission = await service.rebuild_projections(
                run_id=args.run_id,
                names=names,
                idempotency_key=args.idempotency_key
                or f"projection.rebuild:{','.join(args.name) if args.name else 'all'}",
                actor="cli:user",
            )
            if args.jsonl:
                return _runtime_list_result(
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
        return _runtime_list_result(
            "projections",
            projections,
            jsonl=args.jsonl,
        )
    raise ValueError(f"Unknown Impulse runtime command: {args.command}")


def _runtime_list_result(
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


def _runtime_event_schema_report(
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


async def _runtime_run_until_idle(args: argparse.Namespace) -> dict[str, Any]:
    if args.max_ticks < 1:
        raise ValueError("--max-ticks must be greater than zero")
    if args.lease_seconds <= 0:
        raise ValueError("--lease-seconds must be greater than zero")
    backend = Correlator(_cli_db_path(args))
    service = RuntimeBackendService(backend)
    result = await run_until_idle(
        service,
        worker_id=args.worker_id,
        run_id=args.run_id,
        lease_seconds=args.lease_seconds,
        max_ticks=args.max_ticks,
        work_dir=args.work_dir,
        all_runs=args.all_runs,
    )
    return {
        "ok": result.ok,
        "ticks": result.ticks,
        "stopped_reason": result.stopped_reason,
        "completed": [item.model_dump(mode="json") for item in result.completed],
        "failed": [item.model_dump(mode="json") for item in result.failed],
        "waiting": [item.model_dump(mode="json") for item in result.waiting],
    }


async def _runtime_replay_execution(args: argparse.Namespace) -> dict[str, Any]:
    backend = Correlator(_cli_db_path(args))
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
    if not args.rerun and not args.compare:
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
    if process.status != ProcessStatus.succeeded:
        return {
            **base,
            "ok": False,
            "mode": "rerun",
            "error": f"process is not succeeded: {process.status.value}",
        }

    adapter, effector_input, config = _process_effector_request_parts(process)
    if adapter.kind == "manual_homeostat":
        return {
            **base,
            "ok": False,
            "mode": "rerun",
            "error": f"{adapter.kind} processes cannot be execution-rerun locally",
        }
    work_dir = Path(args.work_dir).expanduser() if args.work_dir else None
    if work_dir is not None:
        work_dir.mkdir(parents=True, exist_ok=True)
    result = await create_effector_adapter(adapter.kind).run(
        EffectorRunRequest(
            process_id=process.id,
            impulse_id=process.impulse_id,
            adapter=adapter,
            input=effector_input,
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
    rerun = {
        "output": rerun_output,
        "matches_recorded_output": rerun_output == process.output,
    }
    if args.compare:
        rerun["diff"] = _structural_diff(process.output, rerun_output)
    return {
        **base,
        "mode": "rerun",
        "rerun": rerun,
    }


def _structural_diff(
    recorded: Any, rerun: Any, path: str = "$"
) -> list[dict[str, Any]]:
    """Path-by-path diff of two JSON-able values, leaves only.

    Each entry names the JSONPath-ish location, both values, and whether the
    path was ``added`` (rerun only), ``removed`` (recorded only), or
    ``changed``. Equal values contribute nothing.
    """
    if isinstance(recorded, dict) and isinstance(rerun, dict):
        diffs: list[dict[str, Any]] = []
        for key in sorted(set(recorded) | set(rerun)):
            child = f"{path}.{key}"
            if key not in recorded:
                diffs.append(
                    {"path": child, "change": "added", "recorded": None, "rerun": rerun[key]}
                )
            elif key not in rerun:
                diffs.append(
                    {"path": child, "change": "removed", "recorded": recorded[key], "rerun": None}
                )
            else:
                diffs.extend(_structural_diff(recorded[key], rerun[key], child))
        return diffs
    if isinstance(recorded, list) and isinstance(rerun, list):
        diffs = []
        for index in range(max(len(recorded), len(rerun))):
            child = f"{path}[{index}]"
            if index >= len(recorded):
                diffs.append(
                    {"path": child, "change": "added", "recorded": None, "rerun": rerun[index]}
                )
            elif index >= len(rerun):
                diffs.append(
                    {"path": child, "change": "removed", "recorded": recorded[index], "rerun": None}
                )
            else:
                diffs.extend(_structural_diff(recorded[index], rerun[index], child))
        return diffs
    if recorded != rerun:
        return [{"path": path, "change": "changed", "recorded": recorded, "rerun": rerun}]
    return []


async def _runtime_gc(args: argparse.Namespace) -> dict[str, Any]:
    backend = Correlator(_cli_db_path(args))
    store = FileReactionStore(args.reaction_root)
    cutoff = (
        time.time() - _parse_duration_seconds(args.older_than)
        if args.older_than
        else None
    )
    referenced: set[str] = set()
    all_run_ids = [run.id for run in await backend.list_runs()]
    run_ids = [args.run_id] if args.run_id else all_run_ids
    for run_id in all_run_ids:
        for reaction in await backend.list_reactions(run_id=run_id):
            digest = digest_from_fala_reaction_uri(reaction.uri)
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
        "reaction_root": str(store.root),
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


def _runtime_doctor(args: argparse.Namespace) -> dict[str, Any]:
    db_path = Path(_cli_db_path(args))
    packages = _runtime_package_reports(getattr(args, "packages", []))
    packages_ok = all(package["ok"] for package in packages)
    if args.ensure_schema:
        Correlator(db_path)
    if not db_path.exists():
        report = {
            "ok": False,
            "store_kind": "sqlite",
            "path": str(db_path),
            "error": f"SQLite database does not exist: {db_path}",
            "packages": packages,
        }
        return _write_runtime_doctor_report(args, report)

    with sqlite3.connect(db_path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        missing_tables = sorted(set(_RUNTIME_REQUIRED_TABLES) - tables)
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
                "impulses",
                "runtime_commands",
                "runtime_events",
                "associations",
                "reactions",
                "processes",
                "homeostats",
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
            "required_tables": list(_RUNTIME_REQUIRED_TABLES),
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
    return _write_runtime_doctor_report(args, report)


def _runtime_package_reports(paths: list[str]) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    for raw_path in paths:
        path = Path(raw_path).expanduser()
        try:
            package = load_fala_package_yaml(path)
        except Exception as exc:
            reports.append(
                {
                    "ok": False,
                    "path": str(path),
                    "error": str(exc),
                }
            )
        else:
            adapter_errors = _fala_package_adapter_errors(package)
            reports.append(
                {
                    "ok": not adapter_errors,
                    "path": str(path),
                    "id": package.id,
                    "version": package.version,
                    "impulse_type_count": len(package.impulse_types),
                    "correlation_path_count": len(package.correlation_paths),
                    "adapter_errors": adapter_errors,
                }
            )
    return reports


def _fala_package_adapter_errors(
    package: FalaPackageSpec,
) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    for correlation_path in package.correlation_paths:
        for effector in correlation_path.effectors:
            adapter = effector.adapter
            label = f"{correlation_path.id}.{effector.id}"
            try:
                if adapter.kind == "python_function" and adapter.ref:
                    _resolve_python_ref(adapter.ref)
                if adapter.kind == "subprocess":
                    _validate_subprocess_adapter_reference(adapter)
            except Exception as exc:
                errors.append(
                    {
                        "effector": label,
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


def _validate_subprocess_adapter_reference(adapter: EffectorAdapterSpec) -> None:
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


def _write_runtime_doctor_report(
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


def _add_runtime_db_arg(
    parser: argparse.ArgumentParser,
    *,
    required: bool = True,
    default: str | None = None,
) -> None:
    """Add ``--journal`` / ``--journal-path`` with ``--db`` as SQLite alias."""
    parser.add_argument(
        "--journal",
        "--journal-path",
        dest="journal",
        default=None,
        help="Journal path (SQLite file or sqlite:// URL). Preferred over --db.",
    )
    parser.add_argument(
        "--db",
        dest="db",
        required=False,
        default=default,
        help="Alias for --journal (SQLite reference sink path or sqlite:// URL).",
    )
    if required and default is None:
        # Enforce at resolve time so either flag satisfies the requirement.
        parser.set_defaults(_journal_required=True)


def _cli_db_path(args: argparse.Namespace) -> str:
    """Resolve SQLite journal path from --journal or --db."""
    raw = getattr(args, "journal", None) or getattr(args, "db", None)
    if raw is None or raw == "":
        if getattr(args, "_journal_required", False):
            raise SystemExit("error: one of --journal / --journal-path / --db is required")
        raise SystemExit("error: journal path not provided")
    return _runtime_db_path(str(raw))


def _add_runtime_db_run_args(parser: argparse.ArgumentParser) -> None:
    _add_runtime_db_arg(parser)
    parser.add_argument("--run-id", required=True)


def _sqlite_count(connection: sqlite3.Connection, table: str) -> int:
    return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _runtime_vacuum(db_path: str) -> dict[str, Any]:
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


async def _runtime_trace(args: argparse.Namespace) -> dict[str, Any]:
    backend = Correlator(_cli_db_path(args))
    run = await backend.get_run(run_id=args.run_id)
    events = await backend.list_events(run_id=args.run_id)
    impulses = await backend.list_impulses(run_id=args.run_id)
    impulse_relations = await backend.list_impulse_relations(run_id=args.run_id)
    associations = await backend.list_associations(run_id=args.run_id)
    reactions = await backend.list_reactions(run_id=args.run_id)
    processes = await backend.list_processes(run_id=args.run_id)
    homeostats = await backend.list_homeostats(run_id=args.run_id)
    projections = await backend.list_projections(run_id=args.run_id)
    trace = {
        "run_id": args.run_id,
        "run": run.model_dump(mode="json") if run is not None else None,
        "counts": {
            "reactions": len(reactions),
            "impulse_relations": len(impulse_relations),
            "impulses": len(impulses),
            "events": len(events),
            "homeostats": len(homeostats),
            "associations": len(associations),
            "processes": len(processes),
            "projections": len(projections),
        },
        "timeline": [
            {
                "sequence": event.sequence,
                "type": event.event_type,
                "impulse_id": event.impulse_id,
                "process_id": event.process_id,
                "actor": event.actor,
                "created_at": event.created_at.isoformat(),
            }
            for event in events
        ],
        "events": [event.model_dump(mode="json") for event in events],
        "impulses": [impulse.model_dump(mode="json") for impulse in impulses],
        "impulse_relations": [
            relation.model_dump(mode="json") for relation in impulse_relations
        ],
        "associations": [
            association.model_dump(mode="json") for association in associations
        ],
        "reactions": [reaction.model_dump(mode="json") for reaction in reactions],
        "processes": [process.model_dump(mode="json") for process in processes],
        "homeostats": [homeostat.model_dump(mode="json") for homeostat in homeostats],
        "projections": [
            projection.model_dump(mode="json") for projection in projections
        ],
    }
    return {"ok": True, "trace": trace}


async def _runtime_maintain_journal(args: argparse.Namespace) -> dict[str, Any]:
    service = RuntimeBackendService(Correlator(_cli_db_path(args)))
    reaction_store = _runtime_reaction_store_from_root(Path(args.reaction_root))
    plan = await service.maintain_journal(
        older_than_days=args.older_than_days,
        keep_last=args.keep_last,
        vacuum=not args.no_vacuum,
        dry_run=not args.delete,
        reaction_store=reaction_store,
    )
    return {"ok": True, "maintenance": plan.model_dump(mode="json")}


def _runtime_reaction_store_from_root(root: Path) -> RuntimeReactionStore:
    resolved = root.expanduser().resolve()
    blobs: dict[str, RuntimeReactionBlob] = {}
    blob_root = resolved / "blobs" / "sha256"
    if blob_root.exists():
        for path in sorted(blob_root.glob("*/*")):
            if not path.is_file():
                continue
            digest = path.name.lower()
            if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
                continue
            blobs[digest] = RuntimeReactionBlob(
                digest=digest,
                size_bytes=path.stat().st_size,
                location=str(path),
            )
    return RuntimeReactionStore(root=resolved, blobs=blobs)


async def _runtime_export_html(args: argparse.Namespace) -> dict[str, Any]:
    result = await _runtime_trace(args)
    trace = result["trace"]
    out = Path(args.out).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(_render_runtime_html(trace), encoding="utf-8")
    return {"ok": True, "run_id": args.run_id, "out": str(out)}


async def _runtime_export_bundle(args: argparse.Namespace) -> dict[str, Any]:
    result = await _runtime_trace(args)
    trace = result["trace"]
    out = Path(args.out).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    files = {
        "trace.json": json.dumps(trace, indent=2, sort_keys=True),
        "timeline.json": json.dumps(trace["timeline"], indent=2, sort_keys=True),
        "graph.dot": _render_runtime_dot(trace),
        "report.html": _render_runtime_html(trace),
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


async def _runtime_archive_run(args: argparse.Namespace) -> dict[str, Any]:
    trace_args = argparse.Namespace(
        db=getattr(args, "db", None),
        journal=getattr(args, "journal", None),
        run_id=args.run_id,
    )
    result = await _runtime_trace(trace_args)
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
        "graph.dot": _render_runtime_dot(trace),
        "report.html": _render_runtime_html(trace),
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


def _runtime_archive_gc(args: argparse.Namespace) -> dict[str, Any]:
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


def _render_runtime_html(trace: dict[str, Any]) -> str:
    counts = trace["counts"]
    count_items = "\n".join(
        f"<li><span>{_html(key)}</span><strong>{_html(value)}</strong></li>"
        for key, value in sorted(counts.items())
    )
    event_rows = "\n".join(
        "<tr>"
        f"<td>{_html(item['sequence'])}</td>"
        f"<td>{_html(item['type'])}</td>"
        f"<td>{_html(item['impulse_id'] or '')}</td>"
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
  <title>Fala Impulse Runtime Report - {_html(trace["run_id"])}</title>
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
  <h1>Fala Impulse Runtime Report</h1>
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
        <tr><th>Seq</th><th>Type</th><th>Impulse</th><th>Process</th><th>Actor</th><th>Created</th></tr>
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


def _render_runtime_dot(trace: dict[str, Any]) -> str:
    lines = ["digraph fala_runtime {", "  rankdir=LR;"]
    for impulse in trace["impulses"]:
        label = f"{impulse['id']}\\n{impulse['impulse_type']}"
        lines.append(f"  {_dot_quote(impulse['id'])} [label={_dot_quote(label)}];")
    for relation in trace["impulse_relations"]:
        lines.append(
            "  "
            f"{_dot_quote(relation['source_impulse_id'])} -> "
            f"{_dot_quote(relation['target_impulse_id'])} "
            f"[label={_dot_quote(relation['relation_type'])}];"
        )
    lines.append("}")
    return "\n".join(lines) + "\n"


def _dot_quote(value: Any) -> str:
    return json.dumps(str(value))


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
