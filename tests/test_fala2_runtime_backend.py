from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import textwrap
import tomllib
import unittest
import inspect
import zipfile
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path

from fala.carrier_runtime import FalaRuntime
from fala.cli import main as fala_cli_main
from fala.domain_packs.documents import (
    DocumentCarrierInput,
    carrier_from_document,
    document_from_carrier,
    document_observation,
    document_projection,
)
from fala.domain_packs import splot
from fala.domain_packs.splot import (
    SPLOT_ARBITRATION_CASE,
    SplotArbitrationCase,
    carrier_from_case,
    case_from_carrier,
    case_projection,
    jurisdiction_observation,
    review_gate,
)
from fala.artifacts import FileArtifactStore
from fala.errors import FalaBudgetExceeded
from fala.models import ArtifactRef
from fala.runtime_backend import (
    Artifact,
    BridgeDelivery,
    BridgeDeliveryStatus,
    Carrier,
    CarrierProcessStatus,
    CarrierRunStatus,
    CarrierRelation,
    CarrierType,
    DelegationPolicy,
    EventRef,
    Gate,
    GateStatus,
    Observation,
    Process,
    Projection,
    RuntimeBudget,
    RuntimeCommand,
    RuntimeBackendService,
    RuntimeEvent,
    RuntimePool,
    RuntimeRef,
    Run,
    RunRef,
    SQLITE_RUNTIME_SCHEMA_VERSION,
    SQLiteRuntimeBackend,
)


def _run_cli_json(*args: str) -> dict:
    code, payload = _run_cli_raw(*args)
    if code != 0:
        raise AssertionError(payload)
    return payload


def _run_cli_raw(*args: str) -> tuple[int, dict]:
    buffer = StringIO()
    with redirect_stdout(buffer):
        code = fala_cli_main(list(args))
    payload = json.loads(buffer.getvalue())
    return code, payload


def _carrier_cli_step(request) -> dict:
    return {"value": request.input["value"] + 1}


class Fala2RuntimeBackendTests(unittest.TestCase):
    def test_external_infrastructure_is_not_packaged_with_core(self) -> None:
        pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
        project = tomllib.loads(pyproject.read_text(encoding="utf-8"))["project"]
        dependencies = set(project["dependencies"])

        for package in {
            "boto3",
            "fastapi",
            "httpx",
            "jinja2",
            "python-multipart",
            "redis",
            "uvicorn",
        }:
            self.assertFalse(
                any(dependency.startswith(package) for dependency in dependencies),
                package,
            )
        self.assertNotIn("optional-dependencies", project)

    def test_carrier_core_runs_without_web_api_or_http_client_imports(self) -> None:
        src_dir = Path(__file__).resolve().parents[1] / "src"
        script = textwrap.dedent(
            """
            import asyncio
            import builtins
            import tempfile
            from pathlib import Path

            blocked = {"fastapi", "jinja2", "starlette", "uvicorn", "httpx"}
            original_import = builtins.__import__

            def guarded_import(name, *args, **kwargs):
                if name.split(".", 1)[0] in blocked:
                    raise AssertionError(f"blocked optional import: {name}")
                return original_import(name, *args, **kwargs)

            builtins.__import__ = guarded_import

            from fala import Carrier, FalaRuntime
            from fala.cli import _build_parser as build_cli_parser

            assert build_cli_parser().prog == "fala"

            async def main():
                with tempfile.TemporaryDirectory() as tmp:
                    runtime = FalaRuntime.sqlite(Path(tmp) / "core.sqlite")
                    carrier = Carrier(
                        id="carrier_core",
                        run_id="run_core",
                        carrier_type="case",
                    )
                    stored, submission = await runtime.accept_carrier(
                        carrier,
                        idempotency_key="run_core:carrier.accept:carrier_core",
                    )
                    events = await runtime.list_events(run_id="run_core")
                    assert stored == carrier
                    assert not submission.replayed
                    assert [event.event_type for event in events] == ["carrier.accepted"]

            asyncio.run(main())
            """
        )
        env = {**os.environ, "PYTHONPATH": str(src_dir)}
        completed = subprocess.run(
            [sys.executable, "-c", script],
            check=False,
            env=env,
            text=True,
            capture_output=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_cli_rejects_removed_aliases(self) -> None:
        from fala.cli import _build_parser

        parser = _build_parser()
        removed_commands = [
            "init-document",
            "append-documents",
            "discover-documents",
            "list-documents",
            "list-processes",
            "dead-letter",
            "stuck-work",
            "replay-dead-letter",
            "claim",
            "work-once",
            "complete-process",
            "status",
            "document-lineage",
            "run-results",
            "output-documents",
            "stream-append",
            "stream-list",
            "scaffold",
            "scaffold-blueprints",
            "init-project",
            "package-doctor",
            "package-index",
            "validate",
            "validate-output",
            "validate-context",
            "contract",
            "contract-lint",
            "sync-contracts",
            "inspect-run-input",
            "serve",
            "deployment",
            "worker-deployment",
            "worker-autoscaling",
            "queue-export-claims",
            "queue-run-work",
            "queue-list-work",
            "queue-requeue-work",
            "queue-apply-results",
            "supervise-workers",
            "describe",
        ]
        for command in removed_commands:
            with redirect_stderr(StringIO()):
                with self.assertRaises(SystemExit, msg=command):
                    parser.parse_args([command])
        parser.parse_args(["create-run", "--db", "state.sqlite"])
        with redirect_stderr(StringIO()):
            with self.assertRaises(SystemExit):
                parser.parse_args(["runs", "create", "--db", "state.sqlite"])
        with redirect_stderr(StringIO()):
            with self.assertRaises(SystemExit):
                parser.parse_args(
                    [
                        "gates",
                        "complete",
                        "--db",
                        "state.sqlite",
                        "--run-id",
                        "run_1",
                    ]
                )
        with redirect_stderr(StringIO()):
            with self.assertRaises(SystemExit):
                parser.parse_args(
                    ["gate", "list", "--db", "state.sqlite", "--run-id", "run_1"]
                )
        with redirect_stderr(StringIO()):
            with self.assertRaises(SystemExit):
                parser.parse_args(["queue-list-work", "--queue-db", "queue.sqlite"])
        with redirect_stderr(StringIO()):
            with self.assertRaises(SystemExit):
                parser.parse_args(["doctor", "--carrier-runtime"])

    def test_cli_schema_is_carrier_contract_only(self) -> None:
        from fala.cli import _build_parser

        parser = _build_parser()
        parser.parse_args(["schema", "carrier-package"])
        parser.parse_args(["run-until-idle", "--db", "state.sqlite"])
        removed_models = [
            "document-type",
            "runtime-document-input",
            "process-output",
            "workflow-package",
        ]
        for model in removed_models:
            with redirect_stderr(StringIO()):
                with self.assertRaises(SystemExit, msg=model):
                    parser.parse_args(["schema", model])

    def test_top_level_api_does_not_export_document_core_symbols(self) -> None:
        import fala

        removed = {
            "DocumentRelationSpec",
            "DocumentTypeSpec",
            "RuntimeDocument",
            "RuntimeDocumentInput",
            "SpawnDocumentInput",
            "WorkflowPackageSpec",
            "load_workflow_package_yaml",
            "document_source_value_schema",
            "route_runtime_documents",
        }
        for name in removed:
            self.assertFalse(hasattr(fala, name), name)
            self.assertNotIn(name, fala.__all__)

    def test_sqlite_backend_records_carrier_command_and_ordered_event(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                backend = SQLiteRuntimeBackend(Path(tmp_dir) / "fala2.sqlite")
                carrier = Carrier(
                    run_id="run_alpha",
                    carrier_type="invoice",
                    payload={"amount": 120},
                    metadata={"tenant": "acme"},
                )

                await backend.put_carrier(carrier)
                stored = await backend.get_carrier(
                    run_id="run_alpha", carrier_id=carrier.id
                )

                self.assertEqual(stored, carrier)

                command = RuntimeCommand(
                    run_id="run_alpha",
                    command_type="carrier.accept",
                    idempotency_key="run_alpha:carrier.accept:invoice",
                    actor="operator:mika",
                    correlation_id="corr_1",
                    payload={"carrier_id": carrier.id},
                )
                event = RuntimeEvent(
                    run_id="run_alpha",
                    carrier_id=carrier.id,
                    event_type="carrier.accepted",
                    actor="operator:mika",
                    correlation_id="corr_1",
                    payload={"accepted": True},
                )

                first = await backend.submit_command(command, events=[event])
                replay = await backend.submit_command(
                    command.model_copy(update={"id": "command_duplicate"}),
                    events=[
                        event.model_copy(update={"id": "event_duplicate"}),
                    ],
                )

                self.assertFalse(first.replayed)
                self.assertTrue(replay.replayed)
                self.assertEqual(replay.command.id, first.command.id)
                self.assertEqual(replay.events, [])

                events = await backend.list_events(run_id="run_alpha")
                self.assertEqual(len(events), 1)
                self.assertEqual(events[0].sequence, 1)
                self.assertEqual(events[0].schema_version, 1)
                self.assertEqual(events[0].command_id, first.command.id)
                self.assertEqual(events[0].carrier_id, carrier.id)
                self.assertEqual(events[0].actor, "operator:mika")
                self.assertEqual(events[0].correlation_id, "corr_1")

        asyncio.run(scenario())

    def test_fala_runtime_accepts_non_document_carrier_flow(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                runtime = FalaRuntime.sqlite(Path(tmp_dir) / "fala2.sqlite")
                carrier = Carrier(
                    id="carrier_case_2",
                    run_id="run_case",
                    carrier_type="arbitration_case",
                    payload={"claim_id": "CLM-2"},
                )

                stored, submission = await runtime.accept_carrier(
                    carrier,
                    idempotency_key="run_case:carrier.accept:carrier_case_2",
                )
                events = await runtime.list_events(run_id="run_case")

                self.assertEqual(stored, carrier)
                self.assertFalse(submission.replayed)
                self.assertEqual(carrier.carrier_type, "arbitration_case")
                self.assertNotIn("document_type", carrier.payload)
                self.assertEqual([event.event_type for event in events], ["carrier.accepted"])

        asyncio.run(scenario())

    def test_fala_runtime_registers_carrier_types_and_relations(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                runtime = FalaRuntime.sqlite(Path(tmp_dir) / "fala2.sqlite")
                carrier_type = CarrierType(
                    id="arbitration_case",
                    run_id="run_types",
                    title="Arbitration case",
                    media_types=["application/json"],
                    value_schema={"type": "object"},
                )
                source = Carrier(
                    id="carrier_source",
                    run_id="run_types",
                    carrier_type="arbitration_case",
                )
                target = Carrier(
                    id="carrier_target",
                    run_id="run_types",
                    carrier_type="arbitration_case",
                )
                relation = CarrierRelation(
                    id="relation_derived",
                    run_id="run_types",
                    relation_type="derived_from",
                    source_carrier_id=source.id,
                    target_carrier_id=target.id,
                )

                stored_type, type_submission = await runtime.register_carrier_type(
                    carrier_type,
                    idempotency_key="run_types:carrier_type:arbitration_case",
                )
                replay_type, replay_type_submission = await runtime.register_carrier_type(
                    carrier_type.model_copy(update={"title": "Changed"}),
                    idempotency_key="run_types:carrier_type:arbitration_case",
                )
                await runtime.accept_carrier(
                    source,
                    idempotency_key="run_types:carrier.accept:source",
                )
                await runtime.accept_carrier(
                    target,
                    idempotency_key="run_types:carrier.accept:target",
                )
                stored_relation, relation_submission = (
                    await runtime.record_carrier_relation(
                        relation,
                        idempotency_key="run_types:relation:derived",
                    )
                )
                replay_relation, replay_relation_submission = (
                    await runtime.record_carrier_relation(
                        relation.model_copy(update={"relation_type": "changed"}),
                        idempotency_key="run_types:relation:derived",
                    )
                )

                self.assertEqual(stored_type, carrier_type)
                self.assertEqual(replay_type, carrier_type)
                self.assertFalse(type_submission.replayed)
                self.assertTrue(replay_type_submission.replayed)
                self.assertEqual(stored_relation, relation)
                self.assertEqual(replay_relation, relation)
                self.assertFalse(relation_submission.replayed)
                self.assertTrue(replay_relation_submission.replayed)
                self.assertEqual(
                    await runtime.list_carrier_types(run_id="run_types"),
                    [carrier_type],
                )
                self.assertEqual(
                    await runtime.list_carrier_relations(
                        run_id="run_types",
                        carrier_id=target.id,
                    ),
                    [relation],
                )
                events = await runtime.list_events(run_id="run_types")
                self.assertEqual(
                    [event.event_type for event in events],
                    [
                        "carrier_type.registered",
                        "carrier.accepted",
                        "carrier.accepted",
                        "carrier_relation.recorded",
                    ],
                )

        asyncio.run(scenario())

    def test_fala_runtime_creates_and_transitions_runs(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                runtime = FalaRuntime.sqlite(Path(tmp_dir) / "fala2.sqlite")
                run = Run(
                    id="run_lifecycle",
                    title="Lifecycle",
                    package_id="pkg",
                    package_version="2",
                    flow_id="basic",
                )

                stored, create_submission = await runtime.create_run(
                    run,
                    idempotency_key="run_lifecycle:create",
                    actor="cli:user",
                )
                replayed, replay_submission = await runtime.create_run(
                    run.model_copy(update={"title": "Changed"}),
                    idempotency_key="run_lifecycle:create",
                    actor="cli:user",
                )
                active, active_submission = await runtime.set_run_status(
                    run_id=run.id,
                    status=CarrierRunStatus.active,
                    idempotency_key="run_lifecycle:active",
                )
                completed, completed_submission = await runtime.set_run_status(
                    run_id=run.id,
                    status=CarrierRunStatus.completed,
                    idempotency_key="run_lifecycle:completed",
                )

                self.assertEqual(stored, run)
                self.assertEqual(replayed, run)
                self.assertFalse(create_submission.replayed)
                self.assertTrue(replay_submission.replayed)
                self.assertEqual(active.status, CarrierRunStatus.active)
                self.assertIsNotNone(active.started_at)
                self.assertEqual(completed.status, CarrierRunStatus.completed)
                self.assertIsNotNone(completed.finished_at)
                self.assertFalse(active_submission.replayed)
                self.assertFalse(completed_submission.replayed)
                self.assertEqual(
                    await runtime.list_runs(status=CarrierRunStatus.completed),
                    [completed],
                )
                with self.assertRaisesRegex(ValueError, "terminal"):
                    await runtime.set_run_status(
                        run_id=run.id,
                        status=CarrierRunStatus.active,
                        idempotency_key="run_lifecycle:reopen",
                    )
                events = await runtime.list_events(run_id=run.id)
                self.assertEqual(
                    [event.event_type for event in events],
                    ["run.created", "run.status.changed", "run.status.changed"],
                )

        asyncio.run(scenario())

    def test_fala_runtime_schedules_claims_and_completes_processes(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                runtime = FalaRuntime.sqlite(Path(tmp_dir) / "fala2.sqlite")
                await runtime.create_run(
                    Run(id="run_processes"),
                    idempotency_key="run_processes:create",
                )
                carrier = Carrier(
                    id="carrier_process",
                    run_id="run_processes",
                    carrier_type="case",
                )
                await runtime.accept_carrier(
                    carrier,
                    idempotency_key="run_processes:carrier:carrier_process",
                )
                process = Process(
                    id="process_score",
                    run_id="run_processes",
                    carrier_id=carrier.id,
                    process_type="score",
                    status=CarrierProcessStatus.ready,
                    input={"score": 1},
                )

                scheduled, schedule_submission = await runtime.schedule_process(
                    process,
                    idempotency_key="run_processes:process:score",
                )
                replayed, replay_submission = await runtime.schedule_process(
                    process.model_copy(update={"input": {"score": 2}}),
                    idempotency_key="run_processes:process:score",
                )
                claimed = await runtime.claim_next_ready_process(
                    run_id="run_processes",
                    worker_id="worker_1",
                    lease_seconds=30,
                )
                completed, complete_submission = await runtime.complete_process(
                    run_id="run_processes",
                    process_id=process.id,
                    output={"score": 1},
                    idempotency_key="run_processes:process:score:complete",
                )

                self.assertEqual(scheduled, process)
                self.assertEqual(replayed, process)
                self.assertFalse(schedule_submission.replayed)
                self.assertTrue(replay_submission.replayed)
                self.assertIsNotNone(claimed)
                self.assertEqual(claimed.status, CarrierProcessStatus.running)
                self.assertEqual(claimed.lease_owner, "worker_1")
                self.assertEqual(completed.status, CarrierProcessStatus.succeeded)
                self.assertEqual(completed.output, {"score": 1})
                self.assertFalse(complete_submission.replayed)
                self.assertEqual(
                    await runtime.list_processes(
                        run_id="run_processes",
                        status=CarrierProcessStatus.succeeded,
                    ),
                    [completed],
                )
                events = await runtime.list_events(run_id="run_processes")
                self.assertEqual(
                    [event.event_type for event in events],
                    [
                        "run.created",
                        "carrier.accepted",
                        "process.scheduled",
                        "process.completed",
                    ],
                )
                process_events = [
                    event for event in events if event.event_type.startswith("process.")
                ]
                self.assertEqual(
                    [event.process_id for event in process_events],
                    ["process_score", "process_score"],
                )
                self.assertEqual(
                    [event.carrier_id for event in process_events],
                    ["carrier_process", "carrier_process"],
                )

        asyncio.run(scenario())

    def test_fala_runtime_rebuilds_run_summary_projection(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                runtime = FalaRuntime.sqlite(Path(tmp_dir) / "fala2.sqlite")
                await runtime.create_run(
                    Run(id="run_summary", title="Summary run"),
                    idempotency_key="run_summary:create",
                )
                carrier = Carrier(
                    id="carrier_summary",
                    run_id="run_summary",
                    carrier_type="case",
                    payload={"case_id": "SUM-1"},
                )
                await runtime.accept_carrier(
                    carrier,
                    idempotency_key="run_summary:carrier.accept",
                )
                await runtime.record_observation(
                    Observation(
                        run_id=carrier.run_id,
                        carrier_id=carrier.id,
                        kind="score",
                        values={"score": 1},
                    ),
                    idempotency_key="run_summary:observation.score",
                )
                await runtime.record_artifact(
                    Artifact(
                        id="artifact_summary",
                        run_id=carrier.run_id,
                        carrier_id=carrier.id,
                        kind="report",
                        uri="fala-artifact://sha256/summary",
                    ),
                    idempotency_key="run_summary:artifact.report",
                )
                await runtime.save_gate(
                    Gate(
                        run_id=carrier.run_id,
                        carrier_id=carrier.id,
                        kind="review",
                        status=GateStatus.open,
                    ),
                    idempotency_key="run_summary:gate.review",
                )
                await runtime.schedule_process(
                    Process(
                        id="process_summary",
                        run_id=carrier.run_id,
                        carrier_id=carrier.id,
                        process_type="score",
                        status=CarrierProcessStatus.ready,
                    ),
                    idempotency_key="run_summary:process.score",
                )

                rebuilt, submission = await runtime.rebuild_projections(
                    run_id=carrier.run_id,
                    idempotency_key="run_summary:projection.rebuild",
                )
                self.assertFalse(submission.replayed)
                self.assertEqual(len(rebuilt), 1)
                summary = rebuilt[0]
                self.assertEqual(summary.name, "run_summary")
                self.assertEqual(summary.source_event_sequence, 7)
                self.assertEqual(summary.data["event_count"], 7)
                self.assertEqual(summary.data["carrier_count"], 1)
                self.assertEqual(summary.data["observation_count"], 1)
                self.assertEqual(summary.data["artifact_count"], 1)
                self.assertEqual(summary.data["gate_status_counts"], {"open": 1})
                self.assertEqual(summary.data["process_status_counts"], {"ready": 1})
                self.assertEqual(
                    summary.data["resource_accounting"]["artifact_bytes"],
                    0,
                )
                self.assertEqual(
                    summary.data["resource_accounting"]["process_attempts"],
                    0,
                )
                self.assertEqual(
                    summary.data["event_type_counts"]["projection.rebuilt"],
                    1,
                )

                replayed, replay = await runtime.rebuild_projections(
                    run_id=carrier.run_id,
                    idempotency_key="run_summary:projection.rebuild",
                )
                self.assertTrue(replay.replayed)
                self.assertEqual(replayed, rebuilt)

        asyncio.run(scenario())

    def test_fala_runtime_records_file_artifact_in_filesystem_store(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                root = Path(tmp_dir)
                source = root / "report.txt"
                content = b"carrier artifact\n"
                source.write_bytes(content)
                runtime = FalaRuntime.sqlite(root / ".fala" / "state.sqlite")
                await runtime.create_run(
                    Run(id="run_artifact_file"),
                    idempotency_key="run_artifact_file:create",
                )

                artifact, submission = await runtime.record_file_artifact(
                    run_id="run_artifact_file",
                    kind="report",
                    path=source,
                    media_type="text/plain",
                    artifact_store=root / ".fala" / "artifacts",
                    idempotency_key="run_artifact_file:artifact.report",
                )

                digest = hashlib.sha256(content).hexdigest()
                blob = root / ".fala" / "artifacts" / "blobs" / "sha256" / digest[:2] / digest
                stored = await runtime.service.backend.get_artifact(
                    run_id="run_artifact_file",
                    artifact_id=artifact.id,
                )

                self.assertFalse(submission.replayed)
                self.assertTrue(blob.is_file())
                self.assertEqual(blob.read_bytes(), content)
                self.assertEqual(artifact.uri, f"fala-artifact://sha256/{digest}")
                self.assertEqual(artifact.content_hash, f"sha256:{digest}")
                self.assertEqual(artifact.size_bytes, len(content))
                self.assertEqual(artifact.media_type, "text/plain")
                self.assertEqual(stored, artifact)

        asyncio.run(scenario())

    def test_cli_gc_removes_unreferenced_filesystem_artifact_blobs(self) -> None:
        async def setup(root: Path) -> tuple[Artifact, ArtifactRef]:
            runtime = FalaRuntime.sqlite(root / "state.sqlite")
            run = Run(id="run_gc")
            await runtime.create_run(run, idempotency_key="run_gc:create")
            source = root / "source.txt"
            source.write_text("referenced", encoding="utf-8")
            orphan = root / "orphan.txt"
            orphan.write_text("orphan", encoding="utf-8")
            store = FileArtifactStore(root / "artifacts")

            referenced, _ = await runtime.record_file_artifact(
                run_id=run.id,
                kind="text",
                path=source,
                artifact_store=store,
                idempotency_key="run_gc:artifact:referenced",
            )
            orphan_ref = store.put_file(kind="text", path=orphan)
            return referenced, orphan_ref

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            referenced, orphan_ref = asyncio.run(setup(root))
            store = FileArtifactStore(root / "artifacts")

            dry_run = _run_cli_json(
                "gc",
                "--db",
                str(root / "state.sqlite"),
                "--artifact-root",
                str(root / "artifacts"),
                "--dry-run",
            )
            self.assertEqual(dry_run["collectable"], [orphan_ref.metadata["sha256"]])
            self.assertEqual(dry_run["deleted"], [])

            collected = _run_cli_json(
                "gc",
                "--db",
                str(root / "state.sqlite"),
                "--artifact-root",
                str(root / "artifacts"),
            )
            self.assertEqual(collected["deleted"], [orphan_ref.metadata["sha256"]])
            self.assertTrue(
                store.resolve(
                    ArtifactRef(
                        id=referenced.id,
                        kind=referenced.kind,
                        uri=referenced.uri,
                        metadata=referenced.metadata,
                    )
                ).exists()
            )
            with self.assertRaises(FileNotFoundError):
                store.resolve(orphan_ref)

    def test_cli_inspects_carrier_runtime_state_without_web_stack(self) -> None:
        async def scenario(db_path: Path) -> None:
            runtime = FalaRuntime.sqlite(db_path)
            carrier_type = CarrierType(
                id="case",
                run_id="run_cli",
                title="Case",
                media_types=["application/json"],
            )
            carrier = Carrier(
                id="carrier_cli",
                run_id="run_cli",
                carrier_type="case",
                payload={"case_id": "CLI-1"},
            )
            child = Carrier(
                id="carrier_cli_child",
                run_id="run_cli",
                carrier_type="case",
                payload={"case_id": "CLI-1-child"},
            )
            await runtime.register_carrier_type(
                carrier_type,
                idempotency_key="run_cli:carrier_type:case",
            )
            stored, _ = await runtime.accept_carrier(
                carrier,
                idempotency_key="run_cli:carrier.accept:carrier_cli",
            )
            await runtime.accept_carrier(
                child,
                idempotency_key="run_cli:carrier.accept:carrier_cli_child",
            )
            await runtime.record_carrier_relation(
                CarrierRelation(
                    id="relation_cli",
                    run_id="run_cli",
                    relation_type="derived_from",
                    source_carrier_id=stored.id,
                    target_carrier_id=child.id,
                ),
                idempotency_key="run_cli:carrier_relation:relation_cli",
            )
            await runtime.record_artifact(
                Artifact(
                    id="artifact_cli",
                    run_id="run_cli",
                    carrier_id=stored.id,
                    kind="report",
                    uri="fala-artifact://sha256/abc",
                    media_type="application/json",
                    size_bytes=3,
                    content_hash="sha256:abc",
                ),
                idempotency_key="run_cli:artifact:artifact_cli",
            )
            await runtime.schedule_process(
                Process(
                    id="process_cli",
                    run_id="run_cli",
                    carrier_id=stored.id,
                    process_type="score",
                    status=CarrierProcessStatus.ready,
                    input={"case_id": "CLI-1"},
                ),
                idempotency_key="run_cli:process:process_cli",
            )
            await runtime.record_observation(
                Observation(
                    run_id=stored.run_id,
                    carrier_id=stored.id,
                    kind="score",
                    values={"score": 1},
                ),
                idempotency_key="run_cli:observation.score:carrier_cli",
            )
            await runtime.save_gate(
                Gate(
                    run_id=stored.run_id,
                    carrier_id=stored.id,
                    kind="review",
                    status=GateStatus.open,
                ),
                idempotency_key="run_cli:gate.review:carrier_cli",
            )
            await runtime.save_projection(
                Projection(
                    run_id=stored.run_id,
                    name="case_summary",
                    data={"carrier_id": stored.id},
                    source_event_sequence=1,
                ),
                idempotency_key="run_cli:projection.case_summary",
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "carrier.sqlite"
            created_run = _run_cli_json(
                "create-run",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--title",
                "CLI Run",
                "--metadata",
                "tenant=demo",
            )
            self.assertTrue(created_run["ok"])
            self.assertEqual(created_run["run"]["id"], "run_cli")
            self.assertEqual(created_run["run"]["metadata"], {"tenant": "demo"})

            asyncio.run(scenario(db_path))

            runs = _run_cli_json(
                "runs",
                "list",
                "--db",
                str(db_path),
                "--status",
                "created",
            )
            self.assertEqual(runs["count"], 1)
            self.assertEqual(runs["runs"][0]["id"], "run_cli")

            inspected_run = _run_cli_json(
                "runs",
                "inspect",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
            )
            self.assertTrue(inspected_run["ok"])
            self.assertEqual(inspected_run["run"]["title"], "CLI Run")

            carriers = _run_cli_json(
                "carriers",
                "list",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
            )
            self.assertEqual(carriers["count"], 2)
            self.assertEqual(carriers["carriers"][0]["id"], "carrier_cli")

            inspected = _run_cli_json(
                "carriers",
                "inspect",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--carrier-id",
                "carrier_cli",
            )
            self.assertTrue(inspected["ok"])
            self.assertEqual(inspected["carrier"]["carrier_type"], "case")

            carrier_types = _run_cli_json(
                "carrier-types",
                "list",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
            )
            self.assertEqual(carrier_types["count"], 1)
            self.assertEqual(carrier_types["carrier_types"][0]["id"], "case")

            inspected_type = _run_cli_json(
                "carrier-types",
                "inspect",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--carrier-type-id",
                "case",
            )
            self.assertTrue(inspected_type["ok"])
            self.assertEqual(inspected_type["carrier_type"]["title"], "Case")

            carrier_relations = _run_cli_json(
                "carrier-relations",
                "list",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--carrier-id",
                "carrier_cli_child",
            )
            self.assertEqual(carrier_relations["count"], 1)
            self.assertEqual(
                carrier_relations["carrier_relations"][0]["relation_type"],
                "derived_from",
            )

            inspected_relation = _run_cli_json(
                "carrier-relations",
                "inspect",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--relation-id",
                "relation_cli",
            )
            self.assertTrue(inspected_relation["ok"])
            self.assertEqual(
                inspected_relation["carrier_relation"]["target_carrier_id"],
                "carrier_cli_child",
            )

            artifacts = _run_cli_json(
                "artifacts",
                "list",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--carrier-id",
                "carrier_cli",
                "--kind",
                "report",
            )
            self.assertEqual(artifacts["count"], 1)
            self.assertEqual(artifacts["artifacts"][0]["id"], "artifact_cli")

            inspected_artifact = _run_cli_json(
                "artifacts",
                "inspect",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--artifact-id",
                "artifact_cli",
            )
            self.assertTrue(inspected_artifact["ok"])
            self.assertEqual(inspected_artifact["artifact"]["size_bytes"], 3)

            processes = _run_cli_json(
                "processes",
                "list",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--carrier-id",
                "carrier_cli",
                "--status",
                "ready",
            )
            self.assertEqual(processes["count"], 1)
            self.assertEqual(processes["processes"][0]["id"], "process_cli")

            inspected_process = _run_cli_json(
                "processes",
                "inspect",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--process-id",
                "process_cli",
            )
            self.assertTrue(inspected_process["ok"])
            self.assertEqual(inspected_process["process"]["process_type"], "score")

            events = _run_cli_json(
                "events",
                "list",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--carrier-id",
                "carrier_cli",
            )
            self.assertEqual(
                [event["event_type"] for event in events["events"]],
                [
                    "carrier.accepted",
                    "carrier_relation.recorded",
                    "artifact.recorded",
                    "process.scheduled",
                    "observation.recorded",
                    "gate.saved",
                ],
            )

            observations = _run_cli_json(
                "observations",
                "list",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--carrier-id",
                "carrier_cli",
            )
            self.assertEqual(observations["observations"][0]["kind"], "score")
            inspected_observation = _run_cli_json(
                "observations",
                "inspect",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--observation-id",
                observations["observations"][0]["id"],
            )
            self.assertTrue(inspected_observation["ok"])
            self.assertEqual(
                inspected_observation["observation"]["kind"],
                "score",
            )

            gates = _run_cli_json(
                "gates",
                "list",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--status",
                "open",
            )
            self.assertEqual(gates["gates"][0]["kind"], "review")

            completed_gate = _run_cli_json(
                "gate",
                "complete",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--gate-id",
                gates["gates"][0]["id"],
                "--value",
                "decision=approved",
            )
            self.assertTrue(completed_gate["ok"])
            self.assertEqual(completed_gate["gate"]["status"], "completed")
            self.assertEqual(
                completed_gate["gate"]["values"],
                {"decision": "approved"},
            )

            completed_gates = _run_cli_json(
                "gates",
                "list",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--status",
                "completed",
            )
            self.assertEqual(completed_gates["count"], 1)

            projections = _run_cli_json(
                "projections",
                "list",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
            )
            self.assertEqual(projections["projections"][0]["name"], "case_summary")

            rebuilt = _run_cli_json(
                "projections",
                "rebuild",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
            )
            self.assertEqual(rebuilt["count"], 1)
            summary = rebuilt["projections"][0]
            self.assertEqual(summary["name"], "run_summary")
            self.assertEqual(summary["source_event_sequence"], 12)
            self.assertEqual(summary["data"]["carrier_count"], 2)
            self.assertEqual(summary["data"]["artifact_count"], 1)
            self.assertEqual(
                summary["data"]["resource_accounting"]["artifact_bytes"],
                3,
            )
            self.assertEqual(
                summary["data"]["resource_accounting"]["bridge_delivery_count"],
                0,
            )
            self.assertEqual(
                summary["data"]["event_type_counts"]["projection.rebuilt"],
                1,
            )

            trace = _run_cli_json(
                "trace",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
            )
            payload = trace["trace"]
            self.assertEqual(payload["counts"]["carriers"], 2)
            self.assertEqual(payload["counts"]["events"], 12)
            self.assertEqual(payload["counts"]["projections"], 2)
            self.assertEqual(payload["timeline"][-1]["type"], "projection.rebuilt")
            self.assertEqual(payload["gates"][0]["status"], "completed")

            report_path = Path(tmp_dir) / "report.html"
            exported = _run_cli_json(
                "export-html",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--out",
                str(report_path),
            )
            self.assertTrue(exported["ok"])
            report = report_path.read_text(encoding="utf-8")
            self.assertIn("Fala Carrier Runtime Report", report)
            self.assertIn("projection.rebuilt", report)
            self.assertIn("decision", report)

            bundle_path = Path(tmp_dir) / "run_cli.fala.zip"
            bundle = _run_cli_json(
                "export-bundle",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--out",
                str(bundle_path),
            )
            self.assertTrue(bundle["ok"])
            with zipfile.ZipFile(bundle_path) as archive:
                self.assertEqual(
                    sorted(archive.namelist()),
                    ["graph.dot", "report.html", "timeline.json", "trace.json"],
                )
                graph = archive.read("graph.dot").decode("utf-8")
            self.assertIn('"carrier_cli" -> "carrier_cli_child"', graph)
            self.assertIn("derived_from", graph)

            archive_path = Path(tmp_dir) / "run_cli.archive.zip"
            archived = _run_cli_json(
                "archive-run",
                "run_cli",
                "--db",
                str(db_path),
                "--out",
                str(archive_path),
            )
            self.assertTrue(archived["ok"])
            with zipfile.ZipFile(archive_path) as archive:
                self.assertEqual(
                    sorted(archive.namelist()),
                    [
                        "archive.json",
                        "graph.dot",
                        "report.html",
                        "timeline.json",
                        "trace.json",
                    ],
                )
                archive_json = json.loads(archive.read("archive.json"))
            self.assertEqual(archive_json["format"], "fala-run-archive-v1")
            self.assertEqual(archive_json["run_id"], "run_cli")

    def test_cli_mutates_carriers_observations_and_processes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "carrier.sqlite"
            _run_cli_json(
                "create-run",
                "--db",
                str(db_path),
                "--run-id",
                "run_mutate",
            )

            carrier = _run_cli_json(
                "carriers",
                "create",
                "--db",
                str(db_path),
                "--run-id",
                "run_mutate",
                "--carrier-id",
                "carrier_mutate",
                "--carrier-type",
                "case",
                "--payload-json",
                '{"case_id":"M-1"}',
                "--metadata-json",
                '{"tenant":"demo"}',
            )
            self.assertTrue(carrier["ok"])
            self.assertEqual(carrier["carrier"]["payload"], {"case_id": "M-1"})
            self.assertEqual(carrier["command"]["command_type"], "carrier.accept")

            artifact_path = Path(tmp_dir) / "report.txt"
            artifact_path.write_text("case report", encoding="utf-8")
            artifact = _run_cli_json(
                "artifacts",
                "record",
                "--db",
                str(db_path),
                "--run-id",
                "run_mutate",
                "--artifact-root",
                str(Path(tmp_dir) / "artifacts"),
                "--path",
                str(artifact_path),
                "--kind",
                "report",
                "--carrier-id",
                "carrier_mutate",
                "--media-type",
                "text/plain",
            )
            self.assertTrue(artifact["ok"])
            self.assertEqual(artifact["command"]["command_type"], "artifact.record")
            self.assertEqual(artifact["artifact"]["size_bytes"], 11)

            observation = _run_cli_json(
                "observations",
                "append",
                "--db",
                str(db_path),
                "--run-id",
                "run_mutate",
                "--observation-id",
                "observation_mutate",
                "--carrier-id",
                "carrier_mutate",
                "--kind",
                "score",
                "--values-json",
                '{"score":7}',
            )
            self.assertTrue(observation["ok"])
            self.assertEqual(
                observation["command"]["command_type"],
                "observation.record",
            )

            process = _run_cli_json(
                "processes",
                "schedule",
                "--db",
                str(db_path),
                "--run-id",
                "run_mutate",
                "--process-id",
                "process_mutate",
                "--carrier-id",
                "carrier_mutate",
                "--process-type",
                "score",
                "--status",
                "ready",
                "--input-json",
                '{"value":2}',
            )
            self.assertTrue(process["ok"])
            self.assertEqual(process["process"]["status"], "ready")
            self.assertEqual(process["command"]["command_type"], "process.schedule")

            gate = _run_cli_json(
                "gate",
                "open",
                "--db",
                str(db_path),
                "--run-id",
                "run_mutate",
                "--gate-id",
                "gate_mutate",
                "--carrier-id",
                "carrier_mutate",
                "--kind",
                "human.review",
                "--values-json",
                '{"reason":"manual"}',
            )
            self.assertTrue(gate["ok"])
            self.assertEqual(gate["gate"]["status"], "open")
            self.assertEqual(gate["command"]["command_type"], "gate.open")

            events = _run_cli_json(
                "events",
                "list",
                "--db",
                str(db_path),
                "--run-id",
                "run_mutate",
            )
            self.assertEqual(
                [event["event_type"] for event in events["events"]],
                [
                    "run.created",
                    "carrier.accepted",
                    "artifact.recorded",
                    "observation.recorded",
                    "process.scheduled",
                    "gate.opened",
                ],
            )

    def test_cli_init_and_run_until_idle_execute_carrier_process(self) -> None:
        async def setup(db_path: Path) -> None:
            backend = SQLiteRuntimeBackend(db_path)
            service = RuntimeBackendService(backend)
            await service.create_run(
                Run(id="run_idle"),
                idempotency_key="run_idle:create",
            )
            await service.schedule_process(
                Process(
                    id="process_idle",
                    run_id="run_idle",
                    process_type="python_function",
                    status=CarrierProcessStatus.ready,
                    input={
                        "adapter": {
                            "kind": "python_function",
                            "ref": "tests.test_fala2_runtime_backend._carrier_cli_step",
                        },
                        "value": 2,
                    },
                ),
                idempotency_key="run_idle:process.schedule:process_idle",
            )

        async def inspect(db_path: Path) -> Process:
            backend = SQLiteRuntimeBackend(db_path)
            process = await backend.get_process(
                run_id="run_idle",
                process_id="process_idle",
            )
            assert process is not None
            return process

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            db_path = root / "state.sqlite"
            init = _run_cli_json(
                "init",
                "--db",
                str(db_path),
                "--artifact-root",
                str(root / "artifacts"),
            )
            self.assertTrue(init["ok"])
            self.assertTrue(db_path.exists())
            self.assertTrue((root / "artifacts").exists())

            asyncio.run(setup(db_path))
            result = _run_cli_json(
                "run-until-idle",
                "--db",
                str(db_path),
                "--run-id",
                "run_idle",
            )
            self.assertEqual(result["stopped_reason"], "idle")
            self.assertEqual(len(result["completed"]), 1)
            process = asyncio.run(inspect(db_path))
            self.assertEqual(process.status, CarrierProcessStatus.succeeded)
            self.assertEqual(process.output["value"], 3)

    def test_cli_cancels_carrier_runtime_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "carrier.sqlite"
            _run_cli_json(
                "create-run",
                "--db",
                str(db_path),
                "--run-id",
                "run_cancel",
            )
            cancelled = _run_cli_json(
                "runs",
                "cancel",
                "--db",
                str(db_path),
                "--run-id",
                "run_cancel",
                "--reason",
                "operator requested",
            )
            self.assertTrue(cancelled["ok"])
            self.assertEqual(cancelled["run"]["status"], "cancel_requested")
            self.assertEqual(
                cancelled["command"]["payload"]["reason"],
                "operator requested",
            )
            replay = _run_cli_json(
                "runs",
                "cancel",
                "--db",
                str(db_path),
                "--run-id",
                "run_cancel",
                "--reason",
                "changed",
            )
            self.assertTrue(replay["replayed"])
            self.assertEqual(
                replay["command"]["payload"]["reason"],
                "operator requested",
            )

    def test_document_domain_pack_maps_documents_to_carriers(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                runtime = FalaRuntime.sqlite(Path(tmp_dir) / "fala2.sqlite")
                document = DocumentCarrierInput(
                    id="doc_invoice_1",
                    document_type="invoice_document",
                    title="Invoice 1",
                    media_type="application/pdf",
                    source_uri="file:///tmp/invoice.pdf",
                    values={"vendor": "Acme"},
                    metadata={"tenant": "demo"},
                    artifacts=[
                        {
                            "id": "artifact_pdf",
                            "kind": "pdf",
                            "uri": "file:///tmp/invoice.pdf",
                        }
                    ],
                )
                carrier = carrier_from_document(document, run_id="run_docs")

                stored, _submission = await runtime.accept_carrier(
                    carrier,
                    idempotency_key="run_docs:carrier.accept:doc_invoice_1",
                )
                observation, _ = await runtime.record_observation(
                    document_observation(stored),
                    idempotency_key="run_docs:observation.document:doc_invoice_1",
                )
                projection, _ = await runtime.save_projection(
                    document_projection(stored),
                    idempotency_key="run_docs:projection.document:doc_invoice_1",
                )

                round_trip = document_from_carrier(stored)
                self.assertEqual(round_trip, document)
                self.assertEqual(stored.carrier_type, "document.invoice_document")
                self.assertEqual(stored.metadata["domain_pack"], "documents")
                self.assertEqual(observation.kind, "document.accepted")
                self.assertEqual(observation.values["artifact_count"], 1)
                self.assertEqual(projection.name, "document:doc_invoice_1")
                self.assertEqual(projection.data["document_type"], "invoice_document")

        asyncio.run(scenario())

    def test_splot_domain_pack_uses_public_carrier_runtime_api(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                runtime = FalaRuntime.sqlite(Path(tmp_dir) / "fala2.sqlite")
                case = SplotArbitrationCase(
                    id="splot_case_1",
                    claim_id="SP-1",
                    claimant="Alice",
                    respondent="Beta LLC",
                    amount=1200,
                    currency="EUR",
                    rules="splot-fast-track",
                    artifacts=[
                        {
                            "id": "statement",
                            "kind": "claim_statement",
                            "uri": "file:///tmp/statement.pdf",
                        }
                    ],
                )
                carrier = carrier_from_case(case, run_id="run_splot")

                stored, submission = await runtime.accept_carrier(
                    carrier,
                    idempotency_key="run_splot:carrier.accept:splot_case_1",
                )
                observation, _ = await runtime.record_observation(
                    jurisdiction_observation(
                        stored,
                        admissible=True,
                        reason="contract clause present",
                    ),
                    idempotency_key="run_splot:observation.jurisdiction:splot_case_1",
                )
                gate, _ = await runtime.save_gate(
                    review_gate(stored, status=GateStatus.completed),
                    idempotency_key="run_splot:gate.review:splot_case_1",
                )
                projection, _ = await runtime.save_projection(
                    case_projection(stored),
                    idempotency_key="run_splot:projection.case:splot_case_1",
                )

                self.assertFalse(submission.replayed)
                self.assertEqual(stored.carrier_type, SPLOT_ARBITRATION_CASE)
                self.assertEqual(case_from_carrier(stored), case)
                self.assertEqual(observation.kind, "splot.jurisdiction")
                self.assertEqual(observation.values["admissible"], True)
                self.assertEqual(gate.kind, "splot.review")
                self.assertEqual(gate.status, GateStatus.completed)
                self.assertEqual(projection.name, "splot.case:SP-1")
                self.assertEqual(projection.data["artifact_count"], 1)

        asyncio.run(scenario())

    def test_splot_domain_pack_does_not_use_document_runtime_internals(self) -> None:
        source = inspect.getsource(splot)
        self.assertNotIn("RuntimeDocument", source)
        self.assertNotIn("document_id", source)
        self.assertNotIn("document_type", source)

    def test_runtime_backend_service_accepts_carrier_idempotently(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                service = RuntimeBackendService.sqlite(Path(tmp_dir) / "fala2.sqlite")
                carrier = Carrier(
                    id="carrier_case_1",
                    run_id="run_service",
                    carrier_type="arbitration_case",
                    payload={"claim_id": "CLM-1"},
                )

                first_carrier, first_submission = await service.accept_carrier(
                    carrier,
                    idempotency_key="run_service:carrier.accept:carrier_case_1",
                    actor="operator:mika",
                )
                replay_carrier, replay_submission = await service.accept_carrier(
                    carrier.model_copy(update={"payload": {"claim_id": "changed"}}),
                    idempotency_key="run_service:carrier.accept:carrier_case_1",
                    actor="operator:mika",
                )

                self.assertEqual(first_carrier, carrier)
                self.assertFalse(first_submission.replayed)
                self.assertEqual(replay_carrier, carrier)
                self.assertTrue(replay_submission.replayed)
                self.assertEqual(replay_submission.events, [])
                events = await service.backend.list_events(run_id="run_service")
                self.assertEqual(len(events), 1)
                self.assertEqual(events[0].event_type, "carrier.accepted")

        asyncio.run(scenario())

    def test_runtime_backend_service_replays_gate_and_projection_writes(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                service = RuntimeBackendService.sqlite(Path(tmp_dir) / "fala2.sqlite")
                gate = Gate(
                    id="gate_review",
                    run_id="run_service",
                    kind="human.review",
                    status=GateStatus.completed,
                )
                projection = Projection(
                    id="projection_summary",
                    run_id="run_service",
                    name="summary",
                    version=1,
                    data={"completed_gates": 1},
                    source_event_sequence=1,
                )

                first_gate, first_gate_submission = await service.save_gate(
                    gate,
                    idempotency_key="run_service:gate.save:gate_review",
                    actor="operator:mika",
                )
                replay_gate, replay_gate_submission = await service.save_gate(
                    gate.model_copy(update={"status": GateStatus.cancelled}),
                    idempotency_key="run_service:gate.save:gate_review",
                    actor="operator:mika",
                )
                first_projection, first_projection_submission = (
                    await service.save_projection(
                        projection,
                        idempotency_key="run_service:projection.save:summary",
                        correlation_id="corr_projection",
                    )
                )
                replay_projection, replay_projection_submission = (
                    await service.save_projection(
                        projection.model_copy(update={"version": 2}),
                        idempotency_key="run_service:projection.save:summary",
                        correlation_id="corr_projection",
                    )
                )

                self.assertEqual(first_gate, gate)
                self.assertFalse(first_gate_submission.replayed)
                self.assertEqual(replay_gate, gate)
                self.assertTrue(replay_gate_submission.replayed)
                self.assertEqual(first_projection, projection)
                self.assertFalse(first_projection_submission.replayed)
                self.assertEqual(replay_projection, projection)
                self.assertTrue(replay_projection_submission.replayed)
                events = await service.backend.list_events(run_id="run_service")
                self.assertEqual([event.sequence for event in events], [1, 2])
                self.assertEqual(
                    [event.event_type for event in events],
                    ["gate.saved", "projection.saved"],
                )
                self.assertEqual(events[1].correlation_id, "corr_projection")

        asyncio.run(scenario())

    def test_runtime_backend_service_completes_gate_idempotently(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                service = RuntimeBackendService.sqlite(Path(tmp_dir) / "fala2.sqlite")
                gate = Gate(
                    id="gate_review",
                    run_id="run_service",
                    carrier_id="carrier_review",
                    kind="human.review",
                    status=GateStatus.open,
                )
                await service.save_gate(
                    gate,
                    idempotency_key="run_service:gate.save:gate_review",
                )

                completed, completion = await service.complete_gate(
                    run_id=gate.run_id,
                    gate_id=gate.id,
                    values={"decision": "approved"},
                    idempotency_key="run_service:gate.complete:gate_review",
                    actor="human:jan",
                )
                replayed, replay = await service.complete_gate(
                    run_id=gate.run_id,
                    gate_id=gate.id,
                    values={"decision": "rejected"},
                    idempotency_key="run_service:gate.complete:gate_review",
                    actor="human:jan",
                )

                self.assertFalse(completion.replayed)
                self.assertEqual(completed.status, GateStatus.completed)
                self.assertEqual(completed.values, {"decision": "approved"})
                self.assertTrue(replay.replayed)
                self.assertEqual(replayed, completed)
                events = await service.backend.list_events(run_id=gate.run_id)
                self.assertEqual(
                    [event.event_type for event in events],
                    ["gate.saved", "gate.completed"],
                )
                self.assertEqual(events[1].actor, "human:jan")
                self.assertEqual(events[1].payload["value_keys"], ["decision"])

        asyncio.run(scenario())

    def test_sqlite_backend_records_schema_migration(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "fala2.sqlite"
            SQLiteRuntimeBackend(db_path)
            with sqlite3.connect(db_path) as connection:
                row = connection.execute(
                    """
                    SELECT id, version, name
                    FROM schema_migrations
                    WHERE id = 'runtime_backend'
                    """
                ).fetchone()

        self.assertEqual(
            row,
            ("runtime_backend", SQLITE_RUNTIME_SCHEMA_VERSION, "runtime_backend"),
        )

    def test_cli_db_init_status_and_migrate_manage_carrier_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "fala2.sqlite"

            initialized = _run_cli_json("db", "init", "--db", str(db_path))
            self.assertTrue(initialized["ok"])
            self.assertEqual(initialized["schema_version"], SQLITE_RUNTIME_SCHEMA_VERSION)
            self.assertTrue(db_path.is_file())

            status = _run_cli_json("db", "status", "--db", str(db_path))
            self.assertTrue(status["ok"])
            self.assertEqual(
                status["schema"]["current_version"],
                SQLITE_RUNTIME_SCHEMA_VERSION,
            )
            self.assertEqual(status["schema"]["missing_tables"], [])

            migrated = _run_cli_json("db", "migrate", "--db", str(db_path))
            self.assertTrue(migrated["ok"])
            self.assertEqual(migrated["schema_version"], SQLITE_RUNTIME_SCHEMA_VERSION)

            vacuumed = _run_cli_json("db", "vacuum", "--db", str(db_path))
            self.assertTrue(vacuumed["ok"])
            self.assertEqual(vacuumed["path"], str(db_path))
            self.assertIn("page_count", vacuumed["before"])
            self.assertIn("freelist_count", vacuumed["after"])

    def test_cli_lists_and_inspects_runtime_pools(self) -> None:
        async def scenario(db_path: Path) -> None:
            backend = SQLiteRuntimeBackend(db_path)
            pool = RuntimePool(
                id="local_pool",
                runtimes=[
                    RuntimeRef(id="source", uri="sqlite://source.sqlite"),
                    RuntimeRef(id="target", uri="sqlite://target.sqlite"),
                ],
                carrier_types=["case"],
                metadata={"tenant": "local"},
            )
            policy = DelegationPolicy(
                id="policy_case",
                pool_id=pool.id,
                carrier_types=["case"],
                budget=RuntimeBudget(runtime_hops=1, carrier_count=1, attempts=2),
            )
            await backend.put_runtime_pool(pool)
            await backend.put_delegation_policy(policy)

        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "fala2.sqlite"
            asyncio.run(scenario(db_path))

            listed = _run_cli_json("runtimes", "list", "--db", str(db_path))
            self.assertTrue(listed["ok"])
            self.assertEqual(listed["count"], 1)
            self.assertEqual(listed["runtime_pools"][0]["id"], "local_pool")
            self.assertEqual(
                listed["runtime_pools"][0]["runtimes"][1]["uri"],
                "sqlite://target.sqlite",
            )

            inspected = _run_cli_json(
                "runtimes",
                "inspect",
                "--db",
                str(db_path),
                "--pool-id",
                "local_pool",
            )
            self.assertTrue(inspected["ok"])
            self.assertEqual(inspected["runtime_pool"]["id"], "local_pool")
            self.assertEqual(
                inspected["delegation_policies"][0]["budget"]["runtime_hops"],
                1,
            )

    def test_cli_diagnoses_carrier_runtime_waits_and_deadlocks(self) -> None:
        async def scenario(db_path: Path) -> None:
            runtime = FalaRuntime.sqlite(db_path)
            await runtime.create_run(
                Run(id="run_waits", title="Wait diagnostics"),
                idempotency_key="run_waits:create",
            )
            await runtime.service.backend.put_process(
                Process(
                    id="process_a",
                    run_id="run_waits",
                    process_type="join",
                    status=CarrierProcessStatus.waiting,
                    input={"wait_for_processes": ["process_b"]},
                )
            )
            await runtime.service.backend.put_process(
                Process(
                    id="process_b",
                    run_id="run_waits",
                    process_type="review",
                    status=CarrierProcessStatus.waiting,
                    input={
                        "wait_for_processes": ["process_a"],
                        "wait_for_gates": ["gate_review"],
                    },
                )
            )
            await runtime.service.backend.put_gate(
                Gate(
                    id="gate_review",
                    run_id="run_waits",
                    kind="manual_review",
                    status=GateStatus.open,
                )
            )
            return await runtime.diagnose_waits(run_id="run_waits")

        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "fala2.sqlite"
            direct = asyncio.run(scenario(db_path))
            self.assertTrue(direct.deadlocked)
            self.assertEqual(
                {frozenset(cycle) for cycle in direct.deadlocks},
                {frozenset({"process_a", "process_b"})},
            )

            cli = _run_cli_json("diagnose-waits", "--db", str(db_path), "--run-id", "run_waits")
            diagnostics = cli["wait_diagnostics"]
            self.assertTrue(diagnostics["deadlocked"])
            self.assertEqual(diagnostics["open_gates"], ["gate_review"])
            blocked = {
                item["process_id"]: item for item in diagnostics["blocked"]
            }
            self.assertIn("gate:gate_review", blocked["process_b"]["blocked_by"])

    def test_carrier_runtime_doctor_checks_sqlite_schema(self) -> None:
        async def scenario(db_path: Path) -> None:
            runtime = FalaRuntime.sqlite(db_path)
            await runtime.create_run(
                Run(id="run_doctor", title="Doctor run"),
                idempotency_key="run_doctor:create",
            )
            await runtime.accept_carrier(
                Carrier(
                    id="carrier_doctor",
                    run_id="run_doctor",
                    carrier_type="case",
                ),
                idempotency_key="run_doctor:carrier.accept",
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "missing.sqlite"
            code, missing = _run_cli_raw("doctor", "--db", str(db_path))
            self.assertEqual(code, 1)
            self.assertFalse(missing["ok"])
            self.assertIn("does not exist", missing["error"])

            asyncio.run(scenario(db_path))
            doctor = _run_cli_json("doctor", "--db", str(db_path))
            self.assertTrue(doctor["ok"])
            self.assertEqual(doctor["schema"]["missing_tables"], [])
            self.assertEqual(
                doctor["schema"]["current_version"],
                SQLITE_RUNTIME_SCHEMA_VERSION,
            )
            self.assertEqual(
                doctor["schema"]["latest_version"],
                SQLITE_RUNTIME_SCHEMA_VERSION,
            )
            self.assertEqual(doctor["counts"]["runs"], 1)
            self.assertEqual(doctor["counts"]["carriers"], 1)
            self.assertEqual(doctor["counts"]["runtime_events"], 2)

            output = Path(tmp_dir) / "doctor.json"
            written = _run_cli_json(
                "doctor",
                "--db",
                str(db_path),
                "--output",
                str(output),
            )
            self.assertTrue(written["ok"])
            self.assertEqual(written["current_version"], SQLITE_RUNTIME_SCHEMA_VERSION)
            self.assertTrue(output.is_file())

    def test_sqlite_backend_persists_observations_gates_and_projections(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                backend = SQLiteRuntimeBackend(Path(tmp_dir) / "fala2.sqlite")
                carrier = Carrier(
                    run_id="run_beta",
                    carrier_type="message",
                    payload={"text": "hello"},
                )
                await backend.put_carrier(carrier)

                observation = Observation(
                    run_id="run_beta",
                    carrier_id=carrier.id,
                    kind="classifier.score",
                    values={"score": 0.98},
                    metadata={"model": "local"},
                )
                await backend.put_observation(observation)

                gate = Gate(
                    run_id="run_beta",
                    carrier_id=carrier.id,
                    kind="human.approval",
                    status=GateStatus.open,
                    values={"reason": "needs review"},
                )
                await backend.put_gate(gate)
                completed_gate = await backend.complete_gate(
                    run_id="run_beta",
                    gate_id=gate.id,
                    values={"approved": "yes"},
                )

                projection = Projection(
                    run_id="run_beta",
                    name="carrier_summary",
                    version=1,
                    data={"carrier_count": 1, "last_kind": observation.kind},
                    source_event_sequence=0,
                )
                await backend.put_projection(projection)

                observations = await backend.list_observations(run_id="run_beta")
                stored_gate = await backend.get_gate(run_id="run_beta", gate_id=gate.id)
                stored_projection = await backend.get_projection(
                    run_id="run_beta", name="carrier_summary"
                )
                gates = await backend.list_gates(
                    run_id="run_beta",
                    status=GateStatus.completed,
                )
                projections = await backend.list_projections(run_id="run_beta")

                self.assertEqual(observations, [observation])
                self.assertEqual(stored_gate, completed_gate)
                self.assertEqual(stored_projection, projection)
                self.assertEqual(gates, [completed_gate])
                self.assertEqual(projections, [projection])

        asyncio.run(scenario())

    def test_runtime_backend_service_lists_runtime_systems(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                service = RuntimeBackendService.sqlite(Path(tmp_dir) / "fala2.sqlite")
                carrier = Carrier(
                    run_id="run_query",
                    carrier_type="message",
                    payload={"text": "hello"},
                )
                await service.accept_carrier(
                    carrier,
                    idempotency_key="run_query:carrier.accept:message",
                )
                observation, _ = await service.record_observation(
                    Observation(
                        run_id="run_query",
                        carrier_id=carrier.id,
                        kind="classifier.score",
                        values={"score": 0.98},
                    ),
                    idempotency_key="run_query:observation.record:score",
                )
                gate, _ = await service.save_gate(
                    Gate(
                        run_id="run_query",
                        carrier_id=carrier.id,
                        kind="human.approval",
                        status=GateStatus.open,
                    ),
                    idempotency_key="run_query:gate.save:approval",
                )
                projection, _ = await service.save_projection(
                    Projection(
                        run_id="run_query",
                        name="carrier_summary",
                        data={"carrier_count": 1},
                        source_event_sequence=2,
                    ),
                    idempotency_key="run_query:projection.save:carrier_summary",
                )

                self.assertEqual(
                    await service.list_observations(run_id="run_query"),
                    [observation],
                )
                self.assertEqual(
                    await service.list_gates(
                        run_id="run_query",
                        carrier_id=carrier.id,
                        status=GateStatus.open,
                    ),
                    [gate],
                )
                self.assertEqual(
                    await service.list_projections(run_id="run_query"),
                    [projection],
                )

        asyncio.run(scenario())

    def test_sqlite_bridge_delivers_carrier_between_local_runtimes_idempotently(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                source_path = Path(tmp_dir) / "source.sqlite"
                target_path = Path(tmp_dir) / "target.sqlite"
                source = RuntimeBackendService.sqlite(source_path)
                target = RuntimeBackendService.sqlite(target_path)
                source_ref = RuntimeRef(id="source", uri=f"sqlite://{source_path}")
                target_ref = RuntimeRef(id="target", uri=f"sqlite://{target_path}")
                pool = RuntimePool(
                    id="local_pair",
                    runtimes=[source_ref, target_ref],
                    carrier_types=["case"],
                )
                policy = DelegationPolicy(
                    pool_id=pool.id,
                    carrier_types=["case"],
                    budget=RuntimeBudget(
                        runtime_hops=1,
                        spawned_runs=1,
                        carrier_count=1,
                        wall_time_seconds=30,
                        attempts=2,
                        artifact_bytes=4096,
                    ),
                )
                carrier = Carrier(
                    id="carrier_case",
                    run_id="run_source",
                    carrier_type="case",
                    payload={"claim": "CLM-1"},
                )

                await source.accept_carrier(
                    carrier,
                    idempotency_key="run_source:carrier.accept:carrier_case",
                )
                source_events = await source.backend.list_events(run_id="run_source")
                delivery = BridgeDelivery(
                    id="bridge_case",
                    run_id="run_source",
                    idempotency_key="run_source:bridge:case",
                    source=RunRef(runtime=source_ref, run_id="run_source"),
                    target=RunRef(runtime=target_ref, run_id="run_target"),
                    carrier=carrier,
                    event_ref=EventRef(
                        runtime=source_ref,
                        run_id="run_source",
                        event_id=source_events[0].id,
                        sequence=source_events[0].sequence,
                    ),
                    pool_id=policy.pool_id,
                    budget=policy.budget,
                )

                outbox, enqueue = await source.enqueue_bridge_delivery(delivery)
                replay_outbox, enqueue_replay = await source.enqueue_bridge_delivery(
                    delivery.model_copy(update={"metadata": {"changed": True}}),
                    idempotency_key="run_source:bridge:case",
                )

                self.assertEqual(outbox.pool_id, "local_pair")
                self.assertEqual(outbox.budget.runtime_hops, 1)
                self.assertFalse(enqueue.replayed)
                self.assertEqual(replay_outbox, outbox)
                self.assertTrue(enqueue_replay.replayed)

                delivered, imported, delivered_submission, import_submission = (
                    await source.deliver_bridge_delivery(
                        run_id="run_source",
                        delivery_id="bridge_case",
                        target=target,
                        idempotency_key="run_source:bridge.deliver:case",
                        import_idempotency_key="run_target:bridge.import:case",
                    )
                )
                replay_delivered, replay_imported, delivered_replay, import_replay = (
                    await source.deliver_bridge_delivery(
                        run_id="run_source",
                        delivery_id="bridge_case",
                        target=target,
                        idempotency_key="run_source:bridge.deliver:case",
                        import_idempotency_key="run_target:bridge.import:case",
                    )
                )

                self.assertEqual(delivered.status, BridgeDeliveryStatus.delivered)
                self.assertEqual(imported.status, BridgeDeliveryStatus.imported)
                self.assertEqual(delivered.budget.runtime_hops, 0)
                self.assertEqual(imported.budget.runtime_hops, 0)
                self.assertEqual(delivered.budget.carrier_count, 0)
                self.assertEqual(imported.budget.carrier_count, 0)
                self.assertFalse(delivered_submission.replayed)
                self.assertFalse(import_submission.replayed)
                self.assertEqual(replay_delivered, delivered)
                self.assertEqual(replay_imported, imported)
                self.assertTrue(delivered_replay.replayed)
                self.assertTrue(import_replay.replayed)

                target_carrier = await target.backend.get_carrier(
                    run_id="run_target",
                    carrier_id="carrier_case",
                )
                self.assertIsNotNone(target_carrier)
                assert target_carrier is not None
                self.assertEqual(target_carrier.run_id, "run_target")
                self.assertEqual(
                    target_carrier.metadata["source_runtime_id"],
                    "source",
                )
                self.assertEqual(
                    await source.list_outbox_deliveries(
                        run_id="run_source",
                        status=BridgeDeliveryStatus.delivered,
                    ),
                    [delivered],
                )
                self.assertEqual(
                    await target.list_inbox_deliveries(
                        run_id="run_target",
                        status=BridgeDeliveryStatus.imported,
                    ),
                    [imported],
                )
                self.assertEqual(
                    [event.event_type for event in await source.backend.list_events(run_id="run_source")],
                    [
                        "carrier.accepted",
                        "bridge.outbox.enqueued",
                        "bridge.outbox.delivered",
                    ],
                )
                self.assertEqual(
                    [event.event_type for event in await target.backend.list_events(run_id="run_target")],
                    ["bridge.inbox.imported"],
                )

        asyncio.run(scenario())

    def test_sqlite_bridge_enforces_attempt_budget(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                source_path = Path(tmp_dir) / "source.sqlite"
                target_path = Path(tmp_dir) / "target.sqlite"
                source = RuntimeBackendService.sqlite(source_path)
                target = RuntimeBackendService.sqlite(target_path)
                source_ref = RuntimeRef(id="source", uri=f"sqlite://{source_path}")
                target_ref = RuntimeRef(id="target", uri=f"sqlite://{target_path}")
                carrier = Carrier(
                    id="carrier_budget",
                    run_id="run_source",
                    carrier_type="case",
                )
                await source.accept_carrier(
                    carrier,
                    idempotency_key="run_source:carrier.accept:carrier_budget",
                )
                source_events = await source.backend.list_events(run_id="run_source")
                delivery = BridgeDelivery(
                    id="bridge_budget",
                    run_id="run_source",
                    idempotency_key="run_source:bridge:budget",
                    source=RunRef(runtime=source_ref, run_id="run_source"),
                    target=RunRef(runtime=target_ref, run_id="run_target"),
                    carrier=carrier,
                    event_ref=EventRef(
                        runtime=source_ref,
                        run_id="run_source",
                        event_id=source_events[0].id,
                        sequence=source_events[0].sequence,
                    ),
                    budget=RuntimeBudget(runtime_hops=1, carrier_count=1, attempts=1),
                    attempts=1,
                )
                await source.backend.put_outbox_delivery(delivery)

                with self.assertRaises(FalaBudgetExceeded):
                    await source.deliver_bridge_delivery(
                        run_id="run_source",
                        delivery_id="bridge_budget",
                        target=target,
                        idempotency_key="run_source:bridge.deliver:budget",
                    )
                self.assertEqual(
                    await target.list_inbox_deliveries(run_id="run_target"),
                    [],
                )

        asyncio.run(scenario())

    def test_cli_delivers_bridge_between_local_carrier_runtimes(self) -> None:
        async def scenario(source_path: Path, target_path: Path) -> None:
            source = RuntimeBackendService.sqlite(source_path)
            source_ref = RuntimeRef(id="source", uri=f"sqlite://{source_path}")
            target_ref = RuntimeRef(id="target", uri=f"sqlite://{target_path}")
            carrier = Carrier(
                id="carrier_cli_bridge",
                run_id="run_source",
                carrier_type="case",
                payload={"claim": "CLI-BRIDGE"},
            )
            await source.accept_carrier(
                carrier,
                idempotency_key="run_source:carrier.accept:carrier_cli_bridge",
            )
            source_events = await source.backend.list_events(run_id="run_source")
            await source.enqueue_bridge_delivery(
                BridgeDelivery(
                    id="bridge_cli",
                    run_id="run_source",
                    idempotency_key="run_source:bridge:cli",
                    source=RunRef(runtime=source_ref, run_id="run_source"),
                    target=RunRef(runtime=target_ref, run_id="run_target"),
                    carrier=carrier,
                    event_ref=EventRef(
                        runtime=source_ref,
                        run_id="run_source",
                        event_id=source_events[0].id,
                        sequence=source_events[0].sequence,
                    ),
                    budget=RuntimeBudget(runtime_hops=1, carrier_count=1),
                ),
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            source_path = Path(tmp_dir) / "source.sqlite"
            target_path = Path(tmp_dir) / "target.sqlite"
            asyncio.run(scenario(source_path, target_path))

            pending = _run_cli_json(
                "bridge",
                "list",
                "--db",
                str(source_path),
                "--run-id",
                "run_source",
            )
            self.assertEqual(pending["count"], 1)
            self.assertEqual(pending["bridge_outbox"][0]["status"], "pending")

            delivered = _run_cli_json(
                "bridge",
                "deliver",
                "--db",
                str(source_path),
                "--run-id",
                "run_source",
                "--delivery-id",
                "bridge_cli",
                "--target-db",
                str(target_path),
            )
            self.assertTrue(delivered["ok"])
            self.assertEqual(delivered["delivered"]["status"], "delivered")
            self.assertEqual(delivered["imported"]["status"], "imported")
            self.assertFalse(delivered["delivery_replayed"])
            self.assertFalse(delivered["import_replayed"])

            replay = _run_cli_json(
                "bridge",
                "deliver",
                "--db",
                str(source_path),
                "--run-id",
                "run_source",
                "--delivery-id",
                "bridge_cli",
                "--target-db",
                str(target_path),
            )
            self.assertTrue(replay["delivery_replayed"])
            self.assertTrue(replay["import_replayed"])

            inbox = _run_cli_json(
                "bridge",
                "list",
                "--db",
                str(target_path),
                "--run-id",
                "run_target",
                "--box",
                "inbox",
                "--status",
                "imported",
            )
            self.assertEqual(inbox["count"], 1)
            self.assertEqual(
                inbox["bridge_inbox"][0]["carrier"]["metadata"]["source_run_id"],
                "run_source",
            )


if __name__ == "__main__":
    unittest.main()
