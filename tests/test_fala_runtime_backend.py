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

from fala.runtime import AutonomousCorrelator
from fala.cli import main as fala_cli_main
from fala.domain_packs import signals, splot
from fala.domain_packs.splot import (
    SPLOT_ARBITRATION_CASE,
    SplotArbitrationCase,
    impulse_from_case,
    case_from_impulse,
    case_projection,
    jurisdiction_association,
    review_homeostat,
)
from fala.domain_packs.signals import (
    SIGNAL_METRIC_SAMPLE,
    SIGNAL_THRESHOLD_READING,
    SignalMetricSample,
    impulse_from_metric_sample,
    metric_sample_from_impulse,
    signal_projection,
    threshold_association,
)
from fala.reactions import FileReactionStore
from fala.errors import FalaBudgetExceeded
from fala.models import ReactionRef
from fala.yaml_loader import load_fala_package_yaml
from fala.runtime_backend import (
    Reaction,
    BridgeDelivery,
    BridgeDeliveryStatus,
    Impulse,
    ProcessStatus,
    RunStatus,
    ImpulseRelation,
    ImpulseType,
    DelegationPolicy,
    EventRef,
    Homeostat,
    HomeostatStatus,
    Association,
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
    Correlator,
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


def _impulse_cli_effector(request) -> dict:
    return {"value": request.input["value"] + 1}


async def _put_test_run(target, run_id: str) -> None:
    backend = getattr(target, "backend", target)
    await backend.put_run(Run(id=run_id))


class AutonomousCorrelatorBackendTests(unittest.TestCase):
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

    def test_impulse_core_runs_without_web_api_or_http_client_imports(self) -> None:
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

            from fala import Impulse, AutonomousCorrelator, Run
            from fala.cli import _build_parser as build_cli_parser

            assert build_cli_parser().prog == "fala"

            async def main():
                with tempfile.TemporaryDirectory() as tmp:
                    runtime = AutonomousCorrelator.sqlite(Path(tmp) / "core.sqlite")
                    await runtime.backend.put_run(Run(id="run_core"))
                    impulse = Impulse(
                        id="impulse_core",
                        run_id="run_core",
                        impulse_type="case",
                    )
                    stored, submission = await runtime.accept_impulse(
                        impulse,
                        idempotency_key="run_core:impulse.accept:impulse_core",
                    )
                    events = await runtime.list_events(run_id="run_core")
                    assert stored == impulse
                    assert not submission.replayed
                    assert [event.event_type for event in events] == ["impulse.accepted"]

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
                        "homeostats",
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
                    ["homeostat", "list", "--db", "state.sqlite", "--run-id", "run_1"]
                )
        with redirect_stderr(StringIO()):
            with self.assertRaises(SystemExit):
                parser.parse_args(["queue-list-work", "--queue-db", "queue.sqlite"])
        with redirect_stderr(StringIO()):
            with self.assertRaises(SystemExit):
                parser.parse_args(["doctor", "--runtime"])

    def test_cli_schema_is_impulse_contract_only(self) -> None:
        from fala.cli import _build_parser

        parser = _build_parser()
        parser.parse_args(["schema", "fala-package"])
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

    def test_sqlite_backend_records_impulse_command_and_ordered_event(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                backend = Correlator(Path(tmp_dir) / "fala.sqlite")
                await backend.put_run(Run(id="run_alpha"))
                impulse = Impulse(
                    run_id="run_alpha",
                    impulse_type="invoice",
                    payload={"amount": 120},
                    metadata={"tenant": "acme"},
                )

                await backend.put_impulse(impulse)
                stored = await backend.get_impulse(
                    run_id="run_alpha", impulse_id=impulse.id
                )

                self.assertEqual(stored, impulse)

                command = RuntimeCommand(
                    run_id="run_alpha",
                    command_type="impulse.accept",
                    idempotency_key="run_alpha:impulse.accept:invoice",
                    actor="operator:mika",
                    correlation_id="corr_1",
                    payload={"impulse_id": impulse.id},
                )
                event = RuntimeEvent(
                    run_id="run_alpha",
                    impulse_id=impulse.id,
                    event_type="impulse.accepted",
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
                self.assertEqual(events[0].impulse_id, impulse.id)
                self.assertEqual(events[0].actor, "operator:mika")
                self.assertEqual(events[0].correlation_id, "corr_1")

        asyncio.run(scenario())

    def test_sqlite_backend_rejects_unknown_run_command_and_direct_run_create(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                backend = Correlator(Path(tmp_dir) / "fala.sqlite")
                with self.assertRaisesRegex(ValueError, "Unknown run"):
                    await backend.submit_command(
                        RuntimeCommand(
                            run_id="run_missing",
                            command_type="impulse.accept",
                            idempotency_key="run_missing:impulse.accept",
                        )
                    )
                with self.assertRaisesRegex(ValueError, "create_run"):
                    await backend.submit_command(
                        RuntimeCommand(
                            run_id="run_missing",
                            command_type="run.create",
                            idempotency_key="run_missing:create",
                        )
                    )

        asyncio.run(scenario())

    def test_sqlite_backend_rejects_run_scoped_put_for_unknown_run(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                backend = Correlator(Path(tmp_dir) / "fala.sqlite")
                with self.assertRaisesRegex(ValueError, "Unknown run"):
                    await backend.put_impulse(
                        Impulse(
                            run_id="run_missing",
                            impulse_type="case",
                        )
                    )

        asyncio.run(scenario())

    def test_sqlite_runtime_events_are_append_only(self) -> None:
        async def scenario(db_path: Path) -> None:
            backend = Correlator(db_path)
            await backend.put_run(Run(id="run_events"))
            await backend.submit_command(
                RuntimeCommand(
                    run_id="run_events",
                    command_type="event.append",
                    idempotency_key="run_events:event.append",
                ),
                events=[
                    RuntimeEvent(
                        run_id="run_events",
                        event_type="runtime.fact",
                        payload={"value": 1},
                    )
                ],
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "fala.sqlite"
            asyncio.run(scenario(db_path))
            with sqlite3.connect(db_path) as connection:
                with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
                    connection.execute(
                        """
                        UPDATE runtime_events
                        SET payload = ?
                        WHERE run_id = ?
                        """,
                        ('{"value":2}', "run_events"),
                    )
                with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
                    connection.execute(
                        "DELETE FROM runtime_events WHERE run_id = ?",
                        ("run_events",),
                    )

    def test_sqlite_runtime_commands_are_append_only(self) -> None:
        async def scenario(db_path: Path) -> None:
            backend = Correlator(db_path)
            await backend.put_run(Run(id="run_commands"))
            await backend.submit_command(
                RuntimeCommand(
                    run_id="run_commands",
                    command_type="impulse.accept",
                    idempotency_key="run_commands:impulse.accept",
                    payload={"impulse_id": "impulse_1"},
                )
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "fala.sqlite"
            asyncio.run(scenario(db_path))
            with sqlite3.connect(db_path) as connection:
                with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
                    connection.execute(
                        """
                        UPDATE runtime_commands
                        SET payload = ?
                        WHERE run_id = ?
                        """,
                        ('{"impulse_id":"impulse_2"}', "run_commands"),
                    )
                with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
                    connection.execute(
                        "DELETE FROM runtime_commands WHERE run_id = ?",
                        ("run_commands",),
                    )

    def test_cli_events_validate_schema_reports_unsupported_versions(self) -> None:
        async def setup(db_path: Path) -> None:
            backend = Correlator(db_path)
            await backend.put_run(Run(id="run_event_schema"))
            await backend.submit_command(
                RuntimeCommand(
                    run_id="run_event_schema",
                    command_type="event.append",
                    idempotency_key="run_event_schema:event.v1",
                ),
                events=[
                    RuntimeEvent(
                        run_id="run_event_schema",
                        event_type="runtime.v1",
                        schema_version=1,
                    )
                ],
            )
            await backend.submit_command(
                RuntimeCommand(
                    run_id="run_event_schema",
                    command_type="event.append",
                    idempotency_key="run_event_schema:event.v2",
                ),
                events=[
                    RuntimeEvent(
                        run_id="run_event_schema",
                        event_type="runtime.v2",
                        schema_version=2,
                    )
                ],
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "fala.sqlite"
            asyncio.run(setup(db_path))

            strict_code, strict = _run_cli_raw(
                "events",
                "validate-schema",
                "--db",
                str(db_path),
                "--run-id",
                "run_event_schema",
                "--max-schema-version",
                "1",
            )
            compatible = _run_cli_json(
                "events",
                "validate-schema",
                "--db",
                str(db_path),
                "--run-id",
                "run_event_schema",
                "--max-schema-version",
                "2",
            )

        self.assertFalse(strict["ok"])
        self.assertEqual(strict_code, 1)
        self.assertEqual(strict["event_count"], 2)
        self.assertEqual(strict["schema_versions"], {"1": 1, "2": 1})
        self.assertEqual(len(strict["unsupported_events"]), 1)
        self.assertEqual(strict["unsupported_events"][0]["schema_version"], 2)
        self.assertTrue(compatible["ok"])
        self.assertEqual(compatible["unsupported_events"], [])

    def test_fala_runtime_accepts_arbitrary_impulse_types_without_legacy_document_fields(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                runtime = AutonomousCorrelator.sqlite(Path(tmp_dir) / "fala.sqlite")
                await _put_test_run(runtime, "run_case")
                impulse = Impulse(
                    id="impulse_case_2",
                    run_id="run_case",
                    impulse_type="arbitration_case",
                    payload={"claim_id": "CLM-2"},
                )

                stored, submission = await runtime.accept_impulse(
                    impulse,
                    idempotency_key="run_case:impulse.accept:impulse_case_2",
                )
                events = await runtime.list_events(run_id="run_case")

                self.assertEqual(stored, impulse)
                self.assertFalse(submission.replayed)
                self.assertEqual(impulse.impulse_type, "arbitration_case")
                self.assertNotIn("document_type", impulse.payload)
                self.assertEqual([event.event_type for event in events], ["impulse.accepted"])

        asyncio.run(scenario())

    def test_fala_runtime_registers_impulse_types_and_relations(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                runtime = AutonomousCorrelator.sqlite(Path(tmp_dir) / "fala.sqlite")
                await _put_test_run(runtime, "run_types")
                impulse_type = ImpulseType(
                    id="arbitration_case",
                    run_id="run_types",
                    title="Arbitration case",
                    media_types=["application/json"],
                    value_schema={"type": "object"},
                )
                source = Impulse(
                    id="impulse_source",
                    run_id="run_types",
                    impulse_type="arbitration_case",
                )
                target = Impulse(
                    id="impulse_target",
                    run_id="run_types",
                    impulse_type="arbitration_case",
                )
                relation = ImpulseRelation(
                    id="relation_derived",
                    run_id="run_types",
                    relation_type="derived_from",
                    source_impulse_id=source.id,
                    target_impulse_id=target.id,
                )

                stored_type, type_submission = await runtime.register_impulse_type(
                    impulse_type,
                    idempotency_key="run_types:impulse_type:arbitration_case",
                )
                replay_type, replay_type_submission = await runtime.register_impulse_type(
                    impulse_type.model_copy(update={"title": "Changed"}),
                    idempotency_key="run_types:impulse_type:arbitration_case",
                )
                with self.assertRaisesRegex(ValueError, "already exists"):
                    await runtime.register_impulse_type(
                        impulse_type,
                        idempotency_key="run_types:impulse_type:arbitration_case:again",
                    )
                await runtime.accept_impulse(
                    source,
                    idempotency_key="run_types:impulse.accept:source",
                )
                await runtime.accept_impulse(
                    target,
                    idempotency_key="run_types:impulse.accept:target",
                )
                stored_relation, relation_submission = (
                    await runtime.record_impulse_relation(
                        relation,
                        idempotency_key="run_types:relation:derived",
                    )
                )
                replay_relation, replay_relation_submission = (
                    await runtime.record_impulse_relation(
                        relation.model_copy(update={"relation_type": "changed"}),
                        idempotency_key="run_types:relation:derived",
                    )
                )
                with self.assertRaisesRegex(ValueError, "already exists"):
                    await runtime.record_impulse_relation(
                        relation,
                        idempotency_key="run_types:relation:derived:again",
                    )

                self.assertEqual(stored_type, impulse_type)
                self.assertEqual(replay_type, impulse_type)
                self.assertFalse(type_submission.replayed)
                self.assertTrue(replay_type_submission.replayed)
                self.assertEqual(stored_relation, relation)
                self.assertEqual(replay_relation, relation)
                self.assertFalse(relation_submission.replayed)
                self.assertTrue(replay_relation_submission.replayed)
                self.assertEqual(
                    await runtime.list_impulse_types(run_id="run_types"),
                    [impulse_type],
                )
                self.assertEqual(
                    await runtime.list_impulse_relations(
                        run_id="run_types",
                        impulse_id=target.id,
                    ),
                    [relation],
                )
                events = await runtime.list_events(run_id="run_types")
                self.assertEqual(
                    [event.event_type for event in events],
                    [
                        "impulse_type.registered",
                        "impulse.accepted",
                        "impulse.accepted",
                        "impulse_relation.recorded",
                    ],
                )

        asyncio.run(scenario())

    def test_fala_runtime_creates_and_transitions_runs(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                runtime = AutonomousCorrelator.sqlite(Path(tmp_dir) / "fala.sqlite")
                run = Run(
                    id="run_lifecycle",
                    title="Lifecycle",
                    package_id="pkg",
                    package_version="2",
                    correlation_path_id="basic",
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
                with self.assertRaisesRegex(ValueError, "already exists"):
                    await runtime.create_run(
                        run,
                        idempotency_key="run_lifecycle:create-again",
                    )
                active, active_submission = await runtime.set_run_status(
                    run_id=run.id,
                    status=RunStatus.active,
                    idempotency_key="run_lifecycle:active",
                )
                completed, completed_submission = await runtime.set_run_status(
                    run_id=run.id,
                    status=RunStatus.completed,
                    idempotency_key="run_lifecycle:completed",
                )

                self.assertEqual(stored, run)
                self.assertEqual(replayed, run)
                self.assertFalse(create_submission.replayed)
                self.assertTrue(replay_submission.replayed)
                self.assertEqual(active.status, RunStatus.active)
                self.assertIsNotNone(active.started_at)
                self.assertEqual(completed.status, RunStatus.completed)
                self.assertIsNotNone(completed.finished_at)
                self.assertFalse(active_submission.replayed)
                self.assertFalse(completed_submission.replayed)
                self.assertEqual(
                    await runtime.list_runs(status=RunStatus.completed),
                    [completed],
                )
                with self.assertRaisesRegex(ValueError, "terminal"):
                    await runtime.set_run_status(
                        run_id=run.id,
                        status=RunStatus.active,
                        idempotency_key="run_lifecycle:reopen",
                    )
                events = await runtime.list_events(run_id=run.id)
                self.assertEqual(
                    [event.event_type for event in events],
                    ["run.created", "run.status.changed", "run.status.changed"],
                )

        asyncio.run(scenario())

    def test_fala_runtime_validates_run_status_transition_matrix(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                runtime = AutonomousCorrelator.sqlite(Path(tmp_dir) / "fala.sqlite")

                async def create_run(run_id: str) -> None:
                    await runtime.create_run(
                        Run(id=run_id),
                        idempotency_key=f"{run_id}:create",
                    )

                await create_run("run_waiting_active")
                waiting, _ = await runtime.set_run_status(
                    run_id="run_waiting_active",
                    status=RunStatus.waiting,
                    idempotency_key="run_waiting_active:waiting",
                )
                active, _ = await runtime.set_run_status(
                    run_id="run_waiting_active",
                    status=RunStatus.active,
                    idempotency_key="run_waiting_active:active",
                )
                failed, _ = await runtime.set_run_status(
                    run_id="run_waiting_active",
                    status=RunStatus.failed,
                    idempotency_key="run_waiting_active:failed",
                )

                await create_run("run_cancel")
                cancel_requested, _ = await runtime.set_run_status(
                    run_id="run_cancel",
                    status=RunStatus.cancel_requested,
                    idempotency_key="run_cancel:cancel_requested",
                )
                cancelled, _ = await runtime.set_run_status(
                    run_id="run_cancel",
                    status=RunStatus.cancelled,
                    idempotency_key="run_cancel:cancelled",
                )

                await create_run("run_invalid")
                await runtime.set_run_status(
                    run_id="run_invalid",
                    status=RunStatus.cancel_requested,
                    idempotency_key="run_invalid:cancel_requested",
                )
                with self.assertRaisesRegex(ValueError, "Invalid run status transition"):
                    await runtime.set_run_status(
                        run_id="run_invalid",
                        status=RunStatus.active,
                        idempotency_key="run_invalid:active",
                    )

                await create_run("run_terminal")
                await runtime.set_run_status(
                    run_id="run_terminal",
                    status=RunStatus.completed,
                    idempotency_key="run_terminal:completed",
                )
                with self.assertRaisesRegex(ValueError, "terminal"):
                    await runtime.set_run_status(
                        run_id="run_terminal",
                        status=RunStatus.failed,
                        idempotency_key="run_terminal:failed",
                    )

                self.assertEqual(waiting.status, RunStatus.waiting)
                self.assertEqual(active.status, RunStatus.active)
                self.assertEqual(failed.status, RunStatus.failed)
                self.assertEqual(
                    cancel_requested.status,
                    RunStatus.cancel_requested,
                )
                self.assertEqual(cancelled.status, RunStatus.cancelled)

        asyncio.run(scenario())

    def test_fala_runtime_schedules_claims_and_completes_processes(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                runtime = AutonomousCorrelator.sqlite(Path(tmp_dir) / "fala.sqlite")
                await runtime.create_run(
                    Run(id="run_processes"),
                    idempotency_key="run_processes:create",
                )
                impulse = Impulse(
                    id="impulse_process",
                    run_id="run_processes",
                    impulse_type="case",
                )
                await runtime.accept_impulse(
                    impulse,
                    idempotency_key="run_processes:impulse:impulse_process",
                )
                process = Process(
                    id="process_score",
                    run_id="run_processes",
                    impulse_id=impulse.id,
                    process_type="score",
                    status=ProcessStatus.ready,
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
                    actor="worker_1",
                )

                self.assertEqual(scheduled, process)
                self.assertEqual(replayed, process)
                self.assertFalse(schedule_submission.replayed)
                self.assertTrue(replay_submission.replayed)
                self.assertIsNotNone(claimed)
                self.assertEqual(claimed.status, ProcessStatus.running)
                self.assertEqual(claimed.lease_owner, "worker_1")
                self.assertEqual(completed.status, ProcessStatus.succeeded)
                self.assertEqual(completed.output, {"score": 1})
                self.assertFalse(complete_submission.replayed)
                with self.assertRaisesRegex(ValueError, "not running"):
                    await runtime.complete_process(
                        run_id="run_processes",
                        process_id=process.id,
                        output={"score": 2},
                        idempotency_key="run_processes:process:score:complete-again",
                    )
                with self.assertRaisesRegex(ValueError, "cannot be retried"):
                    await runtime.retry_process(
                        run_id="run_processes",
                        process_id=process.id,
                        idempotency_key="run_processes:process:score:retry-after-success",
                    )
                self.assertEqual(
                    await runtime.list_processes(
                        run_id="run_processes",
                        status=ProcessStatus.succeeded,
                    ),
                    [completed],
                )
                events = await runtime.list_events(run_id="run_processes")
                self.assertEqual(
                    [event.event_type for event in events],
                    [
                        "run.created",
                        "impulse.accepted",
                        "process.scheduled",
                        "process.claimed",
                        "process.completed",
                    ],
                )
                process_events = [
                    event for event in events if event.event_type.startswith("process.")
                ]
                self.assertEqual(
                    [event.process_id for event in process_events],
                    ["process_score", "process_score", "process_score"],
                )
                self.assertEqual(
                    [event.impulse_id for event in process_events],
                    ["impulse_process", "impulse_process", "impulse_process"],
                )

        asyncio.run(scenario())

    def test_claim_is_journaled_and_lease_blocks_foreign_actor(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                runtime = AutonomousCorrelator.sqlite(Path(tmp_dir) / "fala.sqlite")
                await runtime.create_run(
                    Run(id="run_lease"),
                    idempotency_key="run.create",
                )
                impulse = Impulse(
                    id="impulse_lease",
                    run_id="run_lease",
                    impulse_type="case",
                )
                await runtime.accept_impulse(
                    impulse,
                    idempotency_key="impulse.accept:impulse_lease",
                )
                await runtime.schedule_process(
                    Process(
                        id="process_lease",
                        run_id="run_lease",
                        impulse_id=impulse.id,
                        process_type="score",
                        status=ProcessStatus.ready,
                    ),
                    idempotency_key="process.schedule:process_lease",
                )
                claimed = await runtime.claim_next_ready_process(
                    run_id="run_lease",
                    worker_id="worker_1",
                )
                assert claimed is not None

                commands = await runtime.backend.list_commands(run_id="run_lease")
                claim_commands = [
                    command
                    for command in commands
                    if command.command_type == "process.claim"
                ]
                self.assertEqual(len(claim_commands), 1)
                self.assertEqual(claim_commands[0].actor, "worker_1")
                self.assertEqual(
                    claim_commands[0].idempotency_key,
                    "process.claim:process_lease:1",
                )

                with self.assertRaisesRegex(ValueError, "lease is held by"):
                    await runtime.complete_process(
                        run_id="run_lease",
                        process_id="process_lease",
                        output={},
                        idempotency_key="process.complete:process_lease:intruder",
                        actor="worker_intruder",
                    )
                completed, _ = await runtime.complete_process(
                    run_id="run_lease",
                    process_id="process_lease",
                    output={"ok": True},
                    idempotency_key="process.complete:process_lease",
                    actor="worker_1",
                )
                self.assertEqual(completed.status, ProcessStatus.succeeded)

        asyncio.run(scenario())

    def test_retry_wait_lease_blocks_foreign_actor(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                runtime = AutonomousCorrelator.sqlite(Path(tmp_dir) / "fala.sqlite")
                await runtime.create_run(
                    Run(id="run_retry_lease"),
                    idempotency_key="run.create",
                )
                impulse = Impulse(
                    id="impulse_retry_lease",
                    run_id="run_retry_lease",
                    impulse_type="case",
                )
                await runtime.accept_impulse(
                    impulse,
                    idempotency_key="impulse.accept:impulse_retry_lease",
                )
                await runtime.schedule_process(
                    Process(
                        id="process_retry_lease",
                        run_id="run_retry_lease",
                        impulse_id=impulse.id,
                        process_type="score",
                        status=ProcessStatus.ready,
                        max_attempts=2,
                    ),
                    idempotency_key="process.schedule:process_retry_lease",
                )
                claimed = await runtime.claim_next_ready_process(
                    run_id="run_retry_lease",
                    worker_id="worker_1",
                )
                assert claimed is not None

                with self.assertRaisesRegex(ValueError, "lease is held by"):
                    await runtime.retry_process(
                        run_id="run_retry_lease",
                        process_id="process_retry_lease",
                        idempotency_key="process.retry:intruder",
                        actor="worker_intruder",
                    )
                retried, _ = await runtime.retry_process(
                    run_id="run_retry_lease",
                    process_id="process_retry_lease",
                    idempotency_key="process.retry:owner",
                    actor="worker_1",
                )
                self.assertEqual(retried.status, ProcessStatus.retry_wait)

        asyncio.run(scenario())

    def test_claim_closes_expired_final_attempt_process(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                runtime = AutonomousCorrelator.sqlite(Path(tmp_dir) / "fala.sqlite")
                await runtime.create_run(
                    Run(id="run_expired"),
                    idempotency_key="run.create",
                )
                impulse = Impulse(
                    id="impulse_expired",
                    run_id="run_expired",
                    impulse_type="case",
                )
                await runtime.accept_impulse(
                    impulse,
                    idempotency_key="impulse.accept:impulse_expired",
                )
                await runtime.schedule_process(
                    Process(
                        id="process_expired",
                        run_id="run_expired",
                        impulse_id=impulse.id,
                        process_type="score",
                        status=ProcessStatus.ready,
                        max_attempts=1,
                    ),
                    idempotency_key="process.schedule:process_expired",
                )
                claimed = await runtime.claim_next_ready_process(
                    run_id="run_expired",
                    worker_id="worker_1",
                    lease_seconds=0.01,
                )
                assert claimed is not None
                await asyncio.sleep(0.05)

                # The crashed final attempt can never be re-claimed; the claim
                # sweep must close it as failed so the run settles.
                reclaimed = await runtime.claim_next_ready_process(
                    run_id="run_expired",
                    worker_id="worker_2",
                )
                self.assertIsNone(reclaimed)
                process = await runtime.backend.get_process(
                    run_id="run_expired",
                    process_id="process_expired",
                )
                assert process is not None
                self.assertEqual(process.status, ProcessStatus.failed)
                self.assertEqual(process.error["type"], "lease_expired")
                failed_events = [
                    event
                    for event in await runtime.list_events(run_id="run_expired")
                    if event.event_type == "process.failed"
                ]
                self.assertEqual(len(failed_events), 1)
                self.assertEqual(failed_events[0].payload["attempt"], 1)

        asyncio.run(scenario())

    def test_fala_runtime_rebuilds_run_summary_projection(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                runtime = AutonomousCorrelator.sqlite(Path(tmp_dir) / "fala.sqlite")
                await runtime.create_run(
                    Run(id="run_summary", title="Summary run"),
                    idempotency_key="run_summary:create",
                )
                impulse = Impulse(
                    id="impulse_summary",
                    run_id="run_summary",
                    impulse_type="case",
                    payload={"case_id": "SUM-1"},
                )
                await runtime.accept_impulse(
                    impulse,
                    idempotency_key="run_summary:impulse.accept",
                )
                await runtime.record_association(
                    Association(
                        run_id=impulse.run_id,
                        impulse_id=impulse.id,
                        kind="score",
                        values={"score": 1},
                    ),
                    idempotency_key="run_summary:association.score",
                )
                await runtime.record_reaction(
                    Reaction(
                        id="reaction_summary",
                        run_id=impulse.run_id,
                        impulse_id=impulse.id,
                        kind="report",
                        uri="fala-reaction://sha256/summary",
                    ),
                    idempotency_key="run_summary:reaction.report",
                )
                await runtime.save_homeostat(
                    Homeostat(
                        run_id=impulse.run_id,
                        impulse_id=impulse.id,
                        kind="review",
                        status=HomeostatStatus.open,
                    ),
                    idempotency_key="run_summary:homeostat.review",
                )
                await runtime.schedule_process(
                    Process(
                        id="process_summary",
                        run_id=impulse.run_id,
                        impulse_id=impulse.id,
                        process_type="score",
                        status=ProcessStatus.ready,
                    ),
                    idempotency_key="run_summary:process.score",
                )

                rebuilt, submission = await runtime.rebuild_projections(
                    run_id=impulse.run_id,
                    idempotency_key="run_summary:projection.rebuild",
                )
                self.assertFalse(submission.replayed)
                self.assertEqual(len(rebuilt), 1)
                summary = rebuilt[0]
                self.assertEqual(summary.name, "run_summary")
                self.assertEqual(summary.source_event_sequence, 7)
                self.assertEqual(summary.data["event_count"], 7)
                self.assertEqual(summary.data["impulse_count"], 1)
                self.assertEqual(summary.data["association_count"], 1)
                self.assertEqual(summary.data["reaction_count"], 1)
                self.assertEqual(summary.data["homeostat_status_counts"], {"open": 1})
                self.assertEqual(summary.data["process_status_counts"], {"ready": 1})
                self.assertEqual(
                    summary.data["resource_accounting"]["reaction_bytes"],
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
                    run_id=impulse.run_id,
                    idempotency_key="run_summary:projection.rebuild",
                )
                self.assertTrue(replay.replayed)
                self.assertEqual(replayed, rebuilt)

        asyncio.run(scenario())

    def test_fala_runtime_records_file_reaction_in_filesystem_store(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                root = Path(tmp_dir)
                source = root / "report.txt"
                content = b"impulse reaction\n"
                source.write_bytes(content)
                runtime = AutonomousCorrelator.sqlite(root / ".fala" / "state.sqlite")
                await runtime.create_run(
                    Run(id="run_reaction_file"),
                    idempotency_key="run_reaction_file:create",
                )

                reaction, submission = await runtime.record_file_reaction(
                    run_id="run_reaction_file",
                    kind="report",
                    path=source,
                    media_type="text/plain",
                    reaction_store=root / ".fala" / "reactions",
                    idempotency_key="run_reaction_file:reaction.report",
                )

                digest = hashlib.sha256(content).hexdigest()
                blob = root / ".fala" / "reactions" / "blobs" / "sha256" / digest[:2] / digest
                stored = await runtime.service.backend.get_reaction(
                    run_id="run_reaction_file",
                    reaction_id=reaction.id,
                )

                self.assertFalse(submission.replayed)
                self.assertTrue(blob.is_file())
                self.assertEqual(blob.read_bytes(), content)
                self.assertEqual(reaction.uri, f"fala-reaction://sha256/{digest}")
                self.assertEqual(reaction.content_hash, f"sha256:{digest}")
                self.assertEqual(reaction.size_bytes, len(content))
                self.assertEqual(reaction.media_type, "text/plain")
                self.assertEqual(stored, reaction)

        asyncio.run(scenario())

    def test_file_reaction_store_rejects_corrupt_existing_blob(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            source = root / "report.txt"
            content = b"reaction payload"
            source.write_bytes(content)
            digest = hashlib.sha256(content).hexdigest()
            store = FileReactionStore(root / "reactions")
            blob = root / "reactions" / "blobs" / "sha256" / digest[:2] / digest
            blob.parent.mkdir(parents=True, exist_ok=True)
            blob.write_bytes(b"corrupt")

            with self.assertRaisesRegex(ValueError, "digest mismatch"):
                store.put_file(kind="report", path=source)

            self.assertEqual(blob.read_bytes(), b"corrupt")

    def test_cli_gc_removes_unreferenced_filesystem_reaction_blobs(self) -> None:
        async def setup(root: Path) -> tuple[Reaction, ReactionRef]:
            runtime = AutonomousCorrelator.sqlite(root / "state.sqlite")
            run = Run(id="run_gc")
            await runtime.create_run(run, idempotency_key="run_gc:create")
            source = root / "source.txt"
            source.write_text("referenced", encoding="utf-8")
            orphan = root / "orphan.txt"
            orphan.write_text("orphan", encoding="utf-8")
            store = FileReactionStore(root / "reactions")

            referenced, _ = await runtime.record_file_reaction(
                run_id=run.id,
                kind="text",
                path=source,
                reaction_store=store,
                idempotency_key="run_gc:reaction:referenced",
            )
            orphan_ref = store.put_file(kind="text", path=orphan)
            return referenced, orphan_ref

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            referenced, orphan_ref = asyncio.run(setup(root))
            store = FileReactionStore(root / "reactions")

            dry_run = _run_cli_json(
                "gc",
                "--db",
                str(root / "state.sqlite"),
                "--reaction-root",
                str(root / "reactions"),
                "--dry-run",
            )
            self.assertEqual(dry_run["collectable"], [orphan_ref.metadata["sha256"]])
            self.assertEqual(dry_run["deleted"], [])

            collected = _run_cli_json(
                "gc",
                "--db",
                str(root / "state.sqlite"),
                "--reaction-root",
                str(root / "reactions"),
            )
            self.assertEqual(collected["deleted"], [orphan_ref.metadata["sha256"]])
            self.assertTrue(
                store.resolve(
                    ReactionRef(
                        id=referenced.id,
                        kind=referenced.kind,
                        uri=referenced.uri,
                        metadata=referenced.metadata,
                    )
                ).exists()
            )
            with self.assertRaises(FileNotFoundError):
                store.resolve(orphan_ref)

    def test_cli_gc_run_scope_keeps_blobs_referenced_by_other_runs(self) -> None:
        async def setup(root: Path) -> tuple[Reaction, ReactionRef]:
            runtime = AutonomousCorrelator.sqlite(root / "state.sqlite")
            await runtime.create_run(Run(id="run_gc"), idempotency_key="run_gc:create")
            await runtime.create_run(
                Run(id="run_keep"),
                idempotency_key="run_keep:create",
            )
            source = root / "shared.txt"
            source.write_text("shared", encoding="utf-8")
            orphan = root / "orphan.txt"
            orphan.write_text("orphan", encoding="utf-8")
            store = FileReactionStore(root / "reactions")

            shared, _ = await runtime.record_file_reaction(
                run_id="run_keep",
                kind="text",
                path=source,
                reaction_store=store,
                idempotency_key="run_keep:reaction:shared",
            )
            orphan_ref = store.put_file(kind="text", path=orphan)
            return shared, orphan_ref

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            shared, orphan_ref = asyncio.run(setup(root))
            store = FileReactionStore(root / "reactions")

            collected = _run_cli_json(
                "gc",
                "--db",
                str(root / "state.sqlite"),
                "--reaction-root",
                str(root / "reactions"),
                "--run-id",
                "run_gc",
            )
            self.assertEqual(collected["run_ids"], ["run_gc"])
            self.assertEqual(
                set(collected["scanned_run_ids"]),
                {"run_gc", "run_keep"},
            )
            self.assertEqual(collected["deleted"], [orphan_ref.metadata["sha256"]])
            self.assertTrue(
                store.resolve(
                    ReactionRef(
                        id=shared.id,
                        kind=shared.kind,
                        uri=shared.uri,
                        metadata=shared.metadata,
                    )
                ).exists()
            )

    def test_cli_inspects_runtime_state_without_web_stack(self) -> None:
        async def scenario(db_path: Path) -> None:
            runtime = AutonomousCorrelator.sqlite(db_path)
            impulse_type = ImpulseType(
                id="case",
                run_id="run_cli",
                title="Case",
                media_types=["application/json"],
            )
            impulse = Impulse(
                id="impulse_cli",
                run_id="run_cli",
                impulse_type="case",
                payload={"case_id": "CLI-1"},
            )
            child = Impulse(
                id="impulse_cli_child",
                run_id="run_cli",
                impulse_type="case",
                payload={"case_id": "CLI-1-child"},
            )
            await runtime.register_impulse_type(
                impulse_type,
                idempotency_key="run_cli:impulse_type:case",
            )
            stored, _ = await runtime.accept_impulse(
                impulse,
                idempotency_key="run_cli:impulse.accept:impulse_cli",
            )
            await runtime.accept_impulse(
                child,
                idempotency_key="run_cli:impulse.accept:impulse_cli_child",
            )
            await runtime.record_impulse_relation(
                ImpulseRelation(
                    id="relation_cli",
                    run_id="run_cli",
                    relation_type="derived_from",
                    source_impulse_id=stored.id,
                    target_impulse_id=child.id,
                ),
                idempotency_key="run_cli:impulse_relation:relation_cli",
            )
            await runtime.record_reaction(
                Reaction(
                    id="reaction_cli",
                    run_id="run_cli",
                    impulse_id=stored.id,
                    kind="report",
                    uri="fala-reaction://sha256/abc",
                    media_type="application/json",
                    size_bytes=3,
                    content_hash="sha256:abc",
                ),
                idempotency_key="run_cli:reaction:reaction_cli",
            )
            await runtime.schedule_process(
                Process(
                    id="process_cli",
                    run_id="run_cli",
                    impulse_id=stored.id,
                    process_type="score",
                    status=ProcessStatus.ready,
                    input={"case_id": "CLI-1"},
                ),
                idempotency_key="run_cli:process:process_cli",
            )
            await runtime.record_association(
                Association(
                    run_id=stored.run_id,
                    impulse_id=stored.id,
                    kind="score",
                    values={"score": 1},
                ),
                idempotency_key="run_cli:association.score:impulse_cli",
            )
            await runtime.save_homeostat(
                Homeostat(
                    run_id=stored.run_id,
                    impulse_id=stored.id,
                    kind="review",
                    status=HomeostatStatus.open,
                ),
                idempotency_key="run_cli:homeostat.review:impulse_cli",
            )
            await runtime.save_projection(
                Projection(
                    run_id=stored.run_id,
                    name="case_summary",
                    data={"impulse_id": stored.id},
                    source_event_sequence=1,
                ),
                idempotency_key="run_cli:projection.case_summary",
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "impulse.sqlite"
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

            cli_commands = _run_cli_json(
                "commands",
                "list",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--actor",
                "cli:user",
            )
            self.assertEqual(cli_commands["count"], 1)
            self.assertEqual(cli_commands["commands"][0]["command_type"], "run.create")

            run_commands = _run_cli_json(
                "commands",
                "list",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--command-type",
                "run.create",
            )
            self.assertEqual(run_commands["count"], 1)
            self.assertEqual(
                run_commands["commands"][0]["id"],
                cli_commands["commands"][0]["id"],
            )

            inspected_command = _run_cli_json(
                "commands",
                "inspect",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--command-id",
                cli_commands["commands"][0]["id"],
            )
            self.assertTrue(inspected_command["ok"])
            self.assertEqual(
                inspected_command["command"]["idempotency_key"],
                "run.create",
            )

            impulses = _run_cli_json(
                "impulses",
                "list",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
            )
            self.assertEqual(impulses["count"], 2)
            self.assertEqual(impulses["impulses"][0]["id"], "impulse_cli")

            inspected = _run_cli_json(
                "impulses",
                "inspect",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--impulse-id",
                "impulse_cli",
            )
            self.assertTrue(inspected["ok"])
            self.assertEqual(inspected["impulse"]["impulse_type"], "case")

            impulse_types = _run_cli_json(
                "impulse-types",
                "list",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
            )
            self.assertEqual(impulse_types["count"], 1)
            self.assertEqual(impulse_types["impulse_types"][0]["id"], "case")

            inspected_type = _run_cli_json(
                "impulse-types",
                "inspect",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--impulse-type-id",
                "case",
            )
            self.assertTrue(inspected_type["ok"])
            self.assertEqual(inspected_type["impulse_type"]["title"], "Case")

            impulse_relations = _run_cli_json(
                "impulse-relations",
                "list",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--impulse-id",
                "impulse_cli_child",
            )
            self.assertEqual(impulse_relations["count"], 1)
            self.assertEqual(
                impulse_relations["impulse_relations"][0]["relation_type"],
                "derived_from",
            )

            inspected_relation = _run_cli_json(
                "impulse-relations",
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
                inspected_relation["impulse_relation"]["target_impulse_id"],
                "impulse_cli_child",
            )

            reactions = _run_cli_json(
                "reactions",
                "list",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--impulse-id",
                "impulse_cli",
                "--kind",
                "report",
            )
            self.assertEqual(reactions["count"], 1)
            self.assertEqual(reactions["reactions"][0]["id"], "reaction_cli")

            inspected_reaction = _run_cli_json(
                "reactions",
                "inspect",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--reaction-id",
                "reaction_cli",
            )
            self.assertTrue(inspected_reaction["ok"])
            self.assertEqual(inspected_reaction["reaction"]["size_bytes"], 3)

            processes = _run_cli_json(
                "processes",
                "list",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--impulse-id",
                "impulse_cli",
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
                "--impulse-id",
                "impulse_cli",
            )
            self.assertEqual(
                [event["event_type"] for event in events["events"]],
                [
                    "impulse.accepted",
                    "impulse_relation.recorded",
                    "reaction.recorded",
                    "process.scheduled",
                    "association.recorded",
                    "homeostat.saved",
                ],
            )

            associations = _run_cli_json(
                "associations",
                "list",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--impulse-id",
                "impulse_cli",
            )
            self.assertEqual(associations["associations"][0]["kind"], "score")
            inspected_association = _run_cli_json(
                "associations",
                "inspect",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--association-id",
                associations["associations"][0]["id"],
            )
            self.assertTrue(inspected_association["ok"])
            self.assertEqual(
                inspected_association["association"]["kind"],
                "score",
            )

            homeostats = _run_cli_json(
                "homeostats",
                "list",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--status",
                "open",
            )
            self.assertEqual(homeostats["homeostats"][0]["kind"], "review")

            completed_homeostat = _run_cli_json(
                "homeostat",
                "complete",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--homeostat-id",
                homeostats["homeostats"][0]["id"],
                "--value",
                "decision=approved",
            )
            self.assertTrue(completed_homeostat["ok"])
            self.assertEqual(completed_homeostat["homeostat"]["status"], "completed")
            self.assertEqual(
                completed_homeostat["homeostat"]["values"],
                {"decision": "approved"},
            )

            completed_homeostats = _run_cli_json(
                "homeostats",
                "list",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--status",
                "completed",
            )
            self.assertEqual(completed_homeostats["count"], 1)

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
            self.assertEqual(summary["data"]["impulse_count"], 2)
            self.assertEqual(summary["data"]["reaction_count"], 1)
            self.assertEqual(
                summary["data"]["resource_accounting"]["reaction_bytes"],
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
            self.assertEqual(payload["counts"]["impulses"], 2)
            self.assertEqual(payload["counts"]["events"], 12)
            self.assertEqual(payload["counts"]["projections"], 2)
            self.assertEqual(payload["timeline"][-1]["type"], "projection.rebuilt")
            self.assertEqual(payload["homeostats"][0]["status"], "completed")

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
            self.assertIn("Fala Impulse Runtime Report", report)
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
            self.assertIn('"impulse_cli" -> "impulse_cli_child"', graph)
            self.assertIn("derived_from", graph)

            archive_path = Path(tmp_dir) / "run_cli.archive.zip"
            archived = _run_cli_json(
                "archive-run",
                "run_cli",
                "--db",
                str(db_path),
                "--out",
                str(archive_path),
                "--retention-days",
                "30",
            )
            self.assertTrue(archived["ok"])
            self.assertEqual(archived["retention"]["retention_days"], 30)
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
            self.assertEqual(archive_json["retention"]["retention_days"], 30)
            self.assertIn("retain_until", archive_json["retention"])

    def test_cli_archive_gc_deletes_expired_run_archives(self) -> None:
        def write_archive(path: Path, retain_until: str) -> None:
            with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr(
                    "archive.json",
                    json.dumps(
                        {
                            "format": "fala-run-archive-v1",
                            "run_id": path.stem,
                            "retention": {"retain_until": retain_until},
                        }
                    ),
                )

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            expired = root / "expired.zip"
            retained = root / "retained.zip"
            write_archive(expired, "2000-01-01T00:00:00Z")
            write_archive(retained, "2999-01-01T00:00:00Z")

            dry_run = _run_cli_json(
                "archive-gc",
                "--archive-root",
                str(root),
                "--dry-run",
            )
            self.assertEqual(dry_run["expired"], [str(expired.resolve())])
            self.assertEqual(dry_run["deleted"], [])
            self.assertTrue(expired.exists())
            self.assertTrue(retained.exists())

            collected = _run_cli_json(
                "archive-gc",
                "--archive-root",
                str(root),
            )
            self.assertEqual(collected["deleted"], [str(expired.resolve())])
            self.assertFalse(expired.exists())
            self.assertTrue(retained.exists())

    def test_cli_mutates_impulses_associations_and_processes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "impulse.sqlite"
            _run_cli_json(
                "create-run",
                "--db",
                str(db_path),
                "--run-id",
                "run_mutate",
            )

            impulse = _run_cli_json(
                "impulses",
                "create",
                "--db",
                str(db_path),
                "--run-id",
                "run_mutate",
                "--impulse-id",
                "impulse_mutate",
                "--impulse-type",
                "case",
                "--payload-json",
                '{"case_id":"M-1"}',
                "--metadata-json",
                '{"tenant":"demo"}',
            )
            self.assertTrue(impulse["ok"])
            self.assertEqual(impulse["impulse"]["payload"], {"case_id": "M-1"})
            self.assertEqual(impulse["command"]["command_type"], "impulse.accept")

            reaction_path = Path(tmp_dir) / "report.txt"
            reaction_path.write_text("case report", encoding="utf-8")
            reaction = _run_cli_json(
                "reactions",
                "record",
                "--db",
                str(db_path),
                "--run-id",
                "run_mutate",
                "--reaction-root",
                str(Path(tmp_dir) / "reactions"),
                "--path",
                str(reaction_path),
                "--kind",
                "report",
                "--impulse-id",
                "impulse_mutate",
                "--media-type",
                "text/plain",
            )
            self.assertTrue(reaction["ok"])
            self.assertEqual(reaction["command"]["command_type"], "reaction.record")
            self.assertEqual(reaction["reaction"]["size_bytes"], 11)

            association = _run_cli_json(
                "associations",
                "append",
                "--db",
                str(db_path),
                "--run-id",
                "run_mutate",
                "--association-id",
                "association_mutate",
                "--impulse-id",
                "impulse_mutate",
                "--kind",
                "score",
                "--values-json",
                '{"score":7}',
            )
            self.assertTrue(association["ok"])
            self.assertEqual(
                association["command"]["command_type"],
                "association.record",
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
                "--impulse-id",
                "impulse_mutate",
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

            cancelled_process = _run_cli_json(
                "processes",
                "cancel",
                "--db",
                str(db_path),
                "--run-id",
                "run_mutate",
                "--process-id",
                "process_mutate",
                "--error-json",
                '{"reason":"operator"}',
            )
            self.assertTrue(cancelled_process["ok"])
            self.assertEqual(cancelled_process["process"]["status"], "cancelled")
            self.assertEqual(
                cancelled_process["command"]["command_type"],
                "process.cancel",
            )

            homeostat = _run_cli_json(
                "homeostat",
                "open",
                "--db",
                str(db_path),
                "--run-id",
                "run_mutate",
                "--homeostat-id",
                "homeostat_mutate",
                "--impulse-id",
                "impulse_mutate",
                "--kind",
                "human.review",
                "--values-json",
                '{"reason":"manual"}',
            )
            self.assertTrue(homeostat["ok"])
            self.assertEqual(homeostat["homeostat"]["status"], "open")
            self.assertEqual(homeostat["command"]["command_type"], "homeostat.open")

            cancelled_homeostat = _run_cli_json(
                "homeostat",
                "cancel",
                "--db",
                str(db_path),
                "--run-id",
                "run_mutate",
                "--homeostat-id",
                "homeostat_mutate",
                "--value",
                "reason=operator",
            )
            self.assertTrue(cancelled_homeostat["ok"])
            self.assertEqual(cancelled_homeostat["homeostat"]["status"], "cancelled")
            self.assertEqual(
                cancelled_homeostat["command"]["command_type"],
                "homeostat.cancel",
            )

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
                    "impulse.accepted",
                    "reaction.recorded",
                    "association.recorded",
                    "process.scheduled",
                    "process.cancelled",
                    "homeostat.opened",
                    "homeostat.cancelled",
                ],
            )

    def test_cli_init_and_run_until_idle_execute_impulse_process(self) -> None:
        async def setup(db_path: Path) -> None:
            backend = Correlator(db_path)
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
                    status=ProcessStatus.ready,
                    input={
                        "adapter": {
                            "kind": "python_function",
                            "ref": "tests.test_fala_runtime_backend._impulse_cli_effector",
                        },
                        "value": 2,
                    },
                ),
                idempotency_key="run_idle:process.schedule:process_idle",
            )

        async def inspect(db_path: Path) -> Process:
            backend = Correlator(db_path)
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
                "--reaction-root",
                str(root / "reactions"),
            )
            self.assertTrue(init["ok"])
            self.assertTrue(db_path.exists())
            self.assertTrue((root / "reactions").exists())

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
            self.assertEqual(process.status, ProcessStatus.succeeded)
            self.assertEqual(process.output["value"], 3)

    def test_cli_run_until_idle_rejects_invalid_lease_seconds(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "state.sqlite"
            code, payload = _run_cli_raw(
                "run-until-idle",
                "--db",
                str(db_path),
                "--lease-seconds",
                "0",
            )
            self.assertEqual(code, 1)
            self.assertFalse(payload["ok"])
            self.assertIn("--lease-seconds", payload["error"])

    def test_cli_replay_execution_returns_recorded_output_and_reruns_deterministic_process(self) -> None:
        async def setup(db_path: Path) -> None:
            service = RuntimeBackendService.sqlite(db_path)
            await service.create_run(
                Run(id="run_replay"),
                idempotency_key="run_replay:create",
            )
            await service.schedule_process(
                Process(
                    id="process_replay",
                    run_id="run_replay",
                    process_type="python_function",
                    status=ProcessStatus.ready,
                    input={
                        "adapter": {
                            "kind": "python_function",
                            "ref": "tests.test_fala_runtime_backend._impulse_cli_effector",
                        },
                        "value": 6,
                    },
                    metadata={"deterministic": True},
                ),
                idempotency_key="run_replay:process.schedule:process_replay",
            )
            await service.backend.put_process(
                Process(
                    id="process_not_deterministic",
                    run_id="run_replay",
                    process_type="python_function",
                    status=ProcessStatus.succeeded,
                    input={},
                    output={"value": 1},
                )
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "state.sqlite"
            asyncio.run(setup(db_path))
            _run_cli_json(
                "run-until-idle",
                "--db",
                str(db_path),
                "--run-id",
                "run_replay",
            )

            recorded = _run_cli_json(
                "replay-execution",
                "--db",
                str(db_path),
                "--run-id",
                "run_replay",
                "--process-id",
                "process_replay",
            )
            self.assertTrue(recorded["ok"])
            self.assertEqual(recorded["mode"], "recorded")
            self.assertTrue(recorded["rerunnable"])
            self.assertEqual(recorded["recorded"]["output"]["value"], 7)

            rerun = _run_cli_json(
                "replay-execution",
                "--db",
                str(db_path),
                "--run-id",
                "run_replay",
                "--process-id",
                "process_replay",
                "--rerun",
            )
            self.assertTrue(rerun["ok"])
            self.assertEqual(rerun["mode"], "rerun")
            self.assertTrue(rerun["rerun"]["matches_recorded_output"])
            self.assertEqual(rerun["rerun"]["output"]["value"], 7)

            code, refused = _run_cli_raw(
                "replay-execution",
                "--db",
                str(db_path),
                "--run-id",
                "run_replay",
                "--process-id",
                "process_not_deterministic",
                "--rerun",
            )
            self.assertEqual(code, 1)
            self.assertFalse(refused["ok"])
            self.assertIn("not marked deterministic", refused["error"])

    def test_cli_replay_execution_reruns_deterministic_subprocess(self) -> None:
        async def setup(db_path: Path, script: Path) -> None:
            service = RuntimeBackendService.sqlite(db_path)
            await service.create_run(
                Run(id="run_subprocess_replay"),
                idempotency_key="run_subprocess_replay:create",
            )
            await service.schedule_process(
                Process(
                    id="process_subprocess_replay",
                    run_id="run_subprocess_replay",
                    process_type="subprocess",
                    status=ProcessStatus.ready,
                    input={
                        "adapter": {
                            "kind": "subprocess",
                            "command": [sys.executable, str(script)],
                        },
                        "value": 8,
                    },
                    metadata={"deterministic": True},
                ),
                idempotency_key=(
                    "run_subprocess_replay:process.schedule:subprocess"
                ),
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            db_path = root / "state.sqlite"
            script = root / "effector.py"
            script.write_text(
                textwrap.dedent(
                    """
                    from __future__ import annotations

                    import json
                    import os
                    from pathlib import Path

                    manifest = json.loads(Path(os.environ["FALA_EFFECTOR_MANIFEST"]).read_text())
                    output = Path(os.environ["FALA_EFFECTOR_OUTPUT_DIR"])
                    output.mkdir(parents=True, exist_ok=True)
                    value = int(manifest["input"]["value"])
                    (output / "result.json").write_text(
                        json.dumps({"value": value + 2}),
                        encoding="utf-8",
                    )
                    """
                ),
                encoding="utf-8",
            )
            asyncio.run(setup(db_path, script))
            _run_cli_json(
                "run-until-idle",
                "--db",
                str(db_path),
                "--run-id",
                "run_subprocess_replay",
            )

            rerun = _run_cli_json(
                "replay-execution",
                "--db",
                str(db_path),
                "--run-id",
                "run_subprocess_replay",
                "--process-id",
                "process_subprocess_replay",
                "--rerun",
            )

        self.assertTrue(rerun["ok"])
        self.assertEqual(rerun["mode"], "rerun")
        self.assertTrue(rerun["rerun"]["matches_recorded_output"])
        self.assertEqual(rerun["rerun"]["output"]["value"], 10)



    def test_cli_cancels_runtime_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "impulse.sqlite"
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
            self.assertEqual(cancelled["command"]["command_type"], "run.cancel")
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
            events = _run_cli_json(
                "events",
                "list",
                "--db",
                str(db_path),
                "--run-id",
                "run_cancel",
            )
            self.assertEqual(
                [event["event_type"] for event in events["events"]],
                ["run.created", "run.cancel_requested"],
            )


    def test_splot_domain_pack_uses_public_runtime_api(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                runtime = AutonomousCorrelator.sqlite(Path(tmp_dir) / "fala.sqlite")
                await _put_test_run(runtime, "run_splot")
                case = SplotArbitrationCase(
                    id="splot_case_1",
                    claim_id="SP-1",
                    claimant="Alice",
                    respondent="Beta LLC",
                    amount=1200,
                    currency="EUR",
                    rules="splot-fast-track",
                    reactions=[
                        {
                            "id": "statement",
                            "kind": "claim_statement",
                            "uri": "file:///tmp/statement.pdf",
                        }
                    ],
                )
                impulse = impulse_from_case(case, run_id="run_splot")

                stored, submission = await runtime.accept_impulse(
                    impulse,
                    idempotency_key="run_splot:impulse.accept:splot_case_1",
                )
                association, _ = await runtime.record_association(
                    jurisdiction_association(
                        stored,
                        admissible=True,
                        reason="contract clause present",
                    ),
                    idempotency_key="run_splot:association.jurisdiction:splot_case_1",
                )
                opened_homeostat, _ = await runtime.open_homeostat(
                    review_homeostat(stored),
                    idempotency_key="run_splot:homeostat.review:splot_case_1",
                )
                homeostat, _ = await runtime.complete_homeostat(
                    run_id=stored.run_id,
                    homeostat_id=opened_homeostat.id,
                    values={"decision": "approved"},
                    idempotency_key="run_splot:homeostat.review.complete:splot_case_1",
                )
                projection, _ = await runtime.save_projection(
                    case_projection(stored),
                    idempotency_key="run_splot:projection.case:splot_case_1",
                )
                events = await runtime.list_events(run_id=stored.run_id)

                self.assertFalse(submission.replayed)
                self.assertEqual(stored.impulse_type, SPLOT_ARBITRATION_CASE)
                self.assertEqual(case_from_impulse(stored), case)
                self.assertEqual(association.kind, "splot.jurisdiction")
                self.assertEqual(association.values["admissible"], True)
                self.assertEqual(homeostat.kind, "splot.review")
                self.assertEqual(homeostat.status, HomeostatStatus.completed)
                self.assertEqual(projection.name, "splot.case:SP-1")
                self.assertEqual(projection.data["reaction_count"], 1)
                self.assertEqual(
                    [event.event_type for event in events],
                    [
                        "impulse.accepted",
                        "association.recorded",
                        "homeostat.opened",
                        "homeostat.completed",
                        "projection.saved",
                    ],
                )

        asyncio.run(scenario())

    def test_splot_domain_pack_package_manifest_is_impulse_first(self) -> None:
        package = load_fala_package_yaml(
            Path("examples/domain-packs/splot/fala-package.yaml")
        )

        self.assertEqual(package.id, "splot_arbitration_basic")
        self.assertEqual(package.impulse_types[0].id, SPLOT_ARBITRATION_CASE)
        self.assertEqual(package.correlation_paths[0].effectors[0].adapter.kind, "manual_homeostat")

    def test_signals_domain_pack_package_manifest_is_impulse_first(self) -> None:
        package = load_fala_package_yaml(
            Path("examples/domain-packs/signals/fala-package.yaml")
        )

        self.assertEqual(package.id, "signals_basic")
        self.assertEqual(package.impulse_types[0].id, SIGNAL_METRIC_SAMPLE)
        self.assertEqual(package.association_kinds[0].id, SIGNAL_THRESHOLD_READING)
        self.assertEqual(package.reaction_kinds[0].id, "signal_report")
        self.assertEqual(package.correlation_paths[0].effectors[0].adapter.kind, "subprocess")

    def test_signals_domain_pack_uses_public_runtime_api(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                runtime = AutonomousCorrelator.sqlite(Path(tmp_dir) / "fala.sqlite")
                await _put_test_run(runtime, "run_signals")
                sample = SignalMetricSample(
                    id="metric_cpu_1",
                    name="cpu.utilization",
                    value=92,
                    unit="percent",
                    values={"host": "worker-1"},
                    metadata={"tenant": "ops"},
                )
                impulse = impulse_from_metric_sample(sample, run_id="run_signals")

                stored, submission = await runtime.accept_impulse(
                    impulse,
                    idempotency_key="run_signals:impulse.accept:metric_cpu_1",
                )
                association, _ = await runtime.record_association(
                    threshold_association(stored),
                    idempotency_key="run_signals:association.threshold:metric_cpu_1",
                )
                projection, _ = await runtime.save_projection(
                    signal_projection(stored, association),
                    idempotency_key="run_signals:projection.signal:metric_cpu_1",
                )
                events = await runtime.list_events(run_id=stored.run_id)

                self.assertFalse(submission.replayed)
                self.assertEqual(stored.impulse_type, SIGNAL_METRIC_SAMPLE)
                self.assertEqual(metric_sample_from_impulse(stored), sample)
                self.assertEqual(stored.metadata["domain_pack"], "signals")
                self.assertEqual(association.kind, SIGNAL_THRESHOLD_READING)
                self.assertEqual(association.values["state"], "critical")
                self.assertEqual(association.values["threshold"], 90)
                self.assertEqual(projection.name, "signal:metric_cpu_1")
                self.assertEqual(projection.data["state"], "critical")
                self.assertEqual(
                    [event.event_type for event in events],
                    [
                        "impulse.accepted",
                        "association.recorded",
                        "projection.saved",
                    ],
                )

        asyncio.run(scenario())

    def test_splot_domain_pack_does_not_use_legacy_document_internals(self) -> None:
        source = inspect.getsource(splot)
        self.assertNotIn("RuntimeDocument", source)
        self.assertNotIn("document_id", source)
        self.assertNotIn("document_type", source)

    def test_signals_domain_pack_does_not_use_legacy_document_internals(self) -> None:
        source = inspect.getsource(signals)
        self.assertNotIn("RuntimeDocument", source)
        self.assertNotIn("document_id", source)
        self.assertNotIn("document_type", source)

    def test_runtime_backend_service_accepts_impulse_idempotently(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                service = RuntimeBackendService.sqlite(Path(tmp_dir) / "fala.sqlite")
                await _put_test_run(service, "run_service")
                impulse = Impulse(
                    id="impulse_case_1",
                    run_id="run_service",
                    impulse_type="arbitration_case",
                    payload={"claim_id": "CLM-1"},
                )

                first_impulse, first_submission = await service.accept_impulse(
                    impulse,
                    idempotency_key="run_service:impulse.accept:impulse_case_1",
                    actor="operator:mika",
                )
                replay_impulse, replay_submission = await service.accept_impulse(
                    impulse.model_copy(update={"payload": {"claim_id": "changed"}}),
                    idempotency_key="run_service:impulse.accept:impulse_case_1",
                    actor="operator:mika",
                )
                with self.assertRaisesRegex(ValueError, "already exists"):
                    await service.accept_impulse(
                        impulse,
                        idempotency_key="run_service:impulse.accept:again",
                    )

                self.assertEqual(first_impulse, impulse)
                self.assertFalse(first_submission.replayed)
                self.assertEqual(replay_impulse, impulse)
                self.assertTrue(replay_submission.replayed)
                self.assertEqual(replay_submission.events, [])
                events = await service.backend.list_events(run_id="run_service")
                self.assertEqual(len(events), 1)
                self.assertEqual(events[0].event_type, "impulse.accepted")

        asyncio.run(scenario())

    def test_runtime_backend_service_records_associations_and_reactions_idempotently(
        self,
    ) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                service = RuntimeBackendService.sqlite(Path(tmp_dir) / "fala.sqlite")
                await _put_test_run(service, "run_observe")
                impulse = Impulse(
                    id="impulse_observe",
                    run_id="run_observe",
                    impulse_type="arbitration_case",
                    payload={"claim_id": "CLM-1"},
                )
                await service.accept_impulse(
                    impulse,
                    idempotency_key="run_observe:impulse.accept:impulse_observe",
                )

                association = Association(
                    id="association_score",
                    run_id=impulse.run_id,
                    impulse_id=impulse.id,
                    kind="score",
                    values={"score": 1},
                )
                first_association, first_association_submission = (
                    await service.record_association(
                        association,
                        idempotency_key="run_observe:association.record:score",
                    )
                )
                replay_association, replay_association_submission = (
                    await service.record_association(
                        association.model_copy(update={"values": {"score": 2}}),
                        idempotency_key="run_observe:association.record:score",
                    )
                )
                with self.assertRaisesRegex(ValueError, "already exists"):
                    await service.record_association(
                        association,
                        idempotency_key="run_observe:association.record:again",
                    )

                reaction = Reaction(
                    id="reaction_report",
                    run_id=impulse.run_id,
                    impulse_id=impulse.id,
                    kind="report",
                    uri="fala-reaction://sha256/report",
                    media_type="application/json",
                    size_bytes=6,
                    content_hash="sha256:report",
                )
                first_reaction, first_reaction_submission = (
                    await service.record_reaction(
                        reaction,
                        idempotency_key="run_observe:reaction.record:report",
                    )
                )
                replay_reaction, replay_reaction_submission = (
                    await service.record_reaction(
                        reaction.model_copy(
                            update={"uri": "fala-reaction://sha256/changed"}
                        ),
                        idempotency_key="run_observe:reaction.record:report",
                    )
                )
                with self.assertRaisesRegex(ValueError, "already exists"):
                    await service.record_reaction(
                        reaction,
                        idempotency_key="run_observe:reaction.record:again",
                    )

                self.assertEqual(first_association, association)
                self.assertFalse(first_association_submission.replayed)
                self.assertEqual(replay_association, association)
                self.assertTrue(replay_association_submission.replayed)
                self.assertEqual(replay_association_submission.events, [])
                self.assertEqual(first_reaction, reaction)
                self.assertFalse(first_reaction_submission.replayed)
                self.assertEqual(replay_reaction, reaction)
                self.assertTrue(replay_reaction_submission.replayed)
                self.assertEqual(replay_reaction_submission.events, [])

        asyncio.run(scenario())

    def test_runtime_backend_service_replays_homeostat_and_projection_writes(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                service = RuntimeBackendService.sqlite(Path(tmp_dir) / "fala.sqlite")
                await _put_test_run(service, "run_service")
                homeostat = Homeostat(
                    id="homeostat_review",
                    run_id="run_service",
                    kind="human.review",
                    status=HomeostatStatus.completed,
                )
                projection = Projection(
                    id="projection_summary",
                    run_id="run_service",
                    name="summary",
                    version=1,
                    data={"completed_homeostats": 1},
                    source_event_sequence=1,
                )

                first_homeostat, first_homeostat_submission = await service.save_homeostat(
                    homeostat,
                    idempotency_key="run_service:homeostat.save:homeostat_review",
                    actor="operator:mika",
                )
                replay_homeostat, replay_homeostat_submission = await service.save_homeostat(
                    homeostat.model_copy(update={"status": HomeostatStatus.cancelled}),
                    idempotency_key="run_service:homeostat.save:homeostat_review",
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

                self.assertEqual(first_homeostat, homeostat)
                self.assertFalse(first_homeostat_submission.replayed)
                self.assertEqual(replay_homeostat, homeostat)
                self.assertTrue(replay_homeostat_submission.replayed)
                self.assertEqual(first_projection, projection)
                self.assertFalse(first_projection_submission.replayed)
                self.assertEqual(replay_projection, projection)
                self.assertTrue(replay_projection_submission.replayed)
                events = await service.backend.list_events(run_id="run_service")
                self.assertEqual([event.sequence for event in events], [1, 2])
                self.assertEqual(
                    [event.event_type for event in events],
                    ["homeostat.saved", "projection.saved"],
                )
                self.assertEqual(events[1].correlation_id, "corr_projection")

        asyncio.run(scenario())

    def test_runtime_backend_service_wait_process_requires_running_status(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                service = RuntimeBackendService.sqlite(Path(tmp_dir) / "fala.sqlite")
                await _put_test_run(service, "run_wait_service")
                await service.schedule_process(
                    Process(
                        id="process_wait",
                        run_id="run_wait_service",
                        process_type="manual_homeostat",
                        status=ProcessStatus.ready,
                    ),
                    idempotency_key="run_wait_service:process.schedule:process_wait",
                )

                with self.assertRaisesRegex(ValueError, "cannot wait from status"):
                    await service.wait_process(
                        run_id="run_wait_service",
                        process_id="process_wait",
                        idempotency_key="run_wait_service:process.wait:process_wait",
                    )

                claimed = await service.claim_next_ready_process(
                    worker_id="worker:test",
                    run_id="run_wait_service",
                )
                assert claimed is not None
                waiting, submission = await service.wait_process(
                    run_id="run_wait_service",
                    process_id="process_wait",
                    output={"status": "waiting"},
                    idempotency_key="run_wait_service:process.wait:process_wait",
                )
                replayed, replay = await service.wait_process(
                    run_id="run_wait_service",
                    process_id="process_wait",
                    output={"status": "changed"},
                    idempotency_key="run_wait_service:process.wait:process_wait",
                )

                self.assertEqual(waiting.status, ProcessStatus.waiting)
                self.assertEqual(waiting.output["status"], "waiting")
                self.assertFalse(submission.replayed)
                self.assertEqual(replayed, waiting)
                self.assertTrue(replay.replayed)
                self.assertEqual(
                    [
                        event.event_type
                        for event in await service.backend.list_events(
                            run_id="run_wait_service"
                        )
                    ],
                    ["process.scheduled", "process.claimed", "process.waiting"],
                )

        asyncio.run(scenario())

    def test_runtime_backend_service_schedule_process_requires_initial_status(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                service = RuntimeBackendService.sqlite(Path(tmp_dir) / "fala.sqlite")
                await _put_test_run(service, "run_process_initial")
                process = Process(
                    id="process_initial",
                    run_id="run_process_initial",
                    process_type="score",
                    status=ProcessStatus.ready,
                )

                scheduled, submission = await service.schedule_process(
                    process,
                    idempotency_key="run_process_initial:process.schedule:initial",
                )
                replayed, replay = await service.schedule_process(
                    process.model_copy(
                        update={"status": ProcessStatus.succeeded}
                    ),
                    idempotency_key="run_process_initial:process.schedule:initial",
                )
                with self.assertRaisesRegex(ValueError, "already exists"):
                    await service.schedule_process(
                        process,
                        idempotency_key="run_process_initial:process.schedule:again",
                    )
                with self.assertRaisesRegex(ValueError, "pending' or 'ready"):
                    await service.schedule_process(
                        Process(
                            id="process_invalid",
                            run_id="run_process_initial",
                            process_type="score",
                            status=ProcessStatus.succeeded,
                        ),
                        idempotency_key="run_process_initial:process.schedule:invalid",
                    )

                self.assertFalse(submission.replayed)
                self.assertEqual(scheduled.status, ProcessStatus.ready)
                self.assertTrue(replay.replayed)
                self.assertEqual(replayed, scheduled)

        asyncio.run(scenario())

    def test_runtime_backend_service_validates_process_transition_matrix(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                service = RuntimeBackendService.sqlite(Path(tmp_dir) / "fala.sqlite")
                await _put_test_run(service, "run_process_matrix")
                process = Process(
                    id="process_matrix",
                    run_id="run_process_matrix",
                    process_type="score",
                    status=ProcessStatus.ready,
                    max_attempts=2,
                )
                await service.schedule_process(
                    process,
                    idempotency_key="run_process_matrix:process.schedule:matrix",
                )

                with self.assertRaisesRegex(ValueError, "not running"):
                    await service.complete_process(
                        run_id=process.run_id,
                        process_id=process.id,
                        idempotency_key="run_process_matrix:process.complete:ready",
                    )
                with self.assertRaisesRegex(ValueError, "not running"):
                    await service.fail_process(
                        run_id=process.run_id,
                        process_id=process.id,
                        idempotency_key="run_process_matrix:process.fail:ready",
                    )
                with self.assertRaisesRegex(ValueError, "cannot wait from status"):
                    await service.wait_process(
                        run_id=process.run_id,
                        process_id=process.id,
                        idempotency_key="run_process_matrix:process.wait:ready",
                    )

                claimed = await service.claim_next_ready_process(
                    run_id=process.run_id,
                    worker_id="worker:matrix",
                )
                assert claimed is not None
                failed, fail_submission = await service.fail_process(
                    run_id=process.run_id,
                    process_id=process.id,
                    error={"message": "temporary"},
                    idempotency_key="run_process_matrix:process.fail:running",
                    actor="worker:matrix",
                )
                retry_wait, retry_submission = await service.retry_process(
                    run_id=process.run_id,
                    process_id=process.id,
                    error={"message": "retry"},
                    idempotency_key="run_process_matrix:process.retry:failed",
                )
                claimed_again = await service.claim_next_ready_process(
                    run_id=process.run_id,
                    worker_id="worker:matrix",
                )
                assert claimed_again is not None
                succeeded, complete_submission = await service.complete_process(
                    run_id=process.run_id,
                    process_id=process.id,
                    output={"score": 1},
                    idempotency_key="run_process_matrix:process.complete:retry",
                    actor="worker:matrix",
                )
                with self.assertRaisesRegex(ValueError, "cannot be retried"):
                    await service.retry_process(
                        run_id=process.run_id,
                        process_id=process.id,
                        idempotency_key="run_process_matrix:process.retry:succeeded",
                    )

                self.assertEqual(failed.status, ProcessStatus.failed)
                self.assertFalse(fail_submission.replayed)
                self.assertEqual(retry_wait.status, ProcessStatus.retry_wait)
                self.assertFalse(retry_submission.replayed)
                self.assertEqual(
                    claimed_again.status,
                    ProcessStatus.running,
                )
                self.assertEqual(claimed_again.attempt, 2)
                self.assertEqual(succeeded.status, ProcessStatus.succeeded)
                self.assertEqual(succeeded.output, {"score": 1})
                self.assertFalse(complete_submission.replayed)
                events = await service.backend.list_events(run_id=process.run_id)
                self.assertEqual(
                    [event.event_type for event in events],
                    [
                        "process.scheduled",
                        "process.claimed",
                        "process.failed",
                        "process.retry_scheduled",
                        "process.claimed",
                        "process.completed",
                    ],
                )

        asyncio.run(scenario())

    def test_runtime_backend_service_cancels_and_times_out_processes(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                service = RuntimeBackendService.sqlite(Path(tmp_dir) / "fala.sqlite")
                await _put_test_run(service, "run_process_stop")
                cancel_process = Process(
                    id="process_cancel",
                    run_id="run_process_stop",
                    process_type="score",
                    status=ProcessStatus.ready,
                )
                timeout_process = Process(
                    id="process_timeout",
                    run_id="run_process_stop",
                    process_type="score",
                    status=ProcessStatus.ready,
                )
                await service.schedule_process(
                    cancel_process,
                    idempotency_key="run_process_stop:process.schedule:cancel",
                )
                await service.schedule_process(
                    timeout_process,
                    idempotency_key="run_process_stop:process.schedule:timeout",
                )

                cancelled, cancel_submission = await service.cancel_process(
                    run_id="run_process_stop",
                    process_id=cancel_process.id,
                    error={"reason": "operator"},
                    idempotency_key="run_process_stop:process.cancel:cancel",
                    actor="cli:user",
                )
                replayed, replay = await service.cancel_process(
                    run_id="run_process_stop",
                    process_id=cancel_process.id,
                    error={"reason": "changed"},
                    idempotency_key="run_process_stop:process.cancel:cancel",
                    actor="cli:user",
                )
                timed_out, timeout_submission = await service.timeout_process(
                    run_id="run_process_stop",
                    process_id=timeout_process.id,
                    error={"reason": "timeout"},
                    idempotency_key="run_process_stop:process.timeout:timeout",
                    actor="system",
                )

                self.assertEqual(cancelled.status, ProcessStatus.cancelled)
                self.assertEqual(cancelled.error, {"reason": "operator"})
                self.assertFalse(cancel_submission.replayed)
                self.assertEqual(replayed, cancelled)
                self.assertTrue(replay.replayed)
                self.assertEqual(timed_out.status, ProcessStatus.timed_out)
                self.assertEqual(timed_out.error, {"reason": "timeout"})
                self.assertFalse(timeout_submission.replayed)
                with self.assertRaisesRegex(ValueError, "terminal"):
                    await service.timeout_process(
                        run_id="run_process_stop",
                        process_id=cancel_process.id,
                        idempotency_key="run_process_stop:process.timeout:cancel",
                    )
                events = await service.backend.list_events(run_id="run_process_stop")
                self.assertEqual(
                    [event.event_type for event in events],
                    [
                        "process.scheduled",
                        "process.scheduled",
                        "process.cancelled",
                        "process.timed_out",
                    ],
                )
                self.assertEqual(events[2].actor, "cli:user")
                self.assertEqual(events[3].actor, "system")

        asyncio.run(scenario())

    def test_runtime_backend_service_open_homeostat_is_create_only(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                service = RuntimeBackendService.sqlite(Path(tmp_dir) / "fala.sqlite")
                await _put_test_run(service, "run_homeostat_open")
                homeostat = Homeostat(
                    id="homeostat_open_once",
                    run_id="run_homeostat_open",
                    kind="human.review",
                    status=HomeostatStatus.open,
                )
                opened, opened_submission = await service.open_homeostat(
                    homeostat,
                    idempotency_key="run_homeostat_open:homeostat.open:homeostat_open_once",
                )
                replayed, replay = await service.open_homeostat(
                    homeostat.model_copy(update={"metadata": {"changed": True}}),
                    idempotency_key="run_homeostat_open:homeostat.open:homeostat_open_once",
                )

                self.assertEqual(opened, homeostat)
                self.assertFalse(opened_submission.replayed)
                self.assertEqual(replayed, homeostat)
                self.assertTrue(replay.replayed)
                with self.assertRaisesRegex(ValueError, "already exists"):
                    await service.open_homeostat(
                        homeostat.model_copy(update={"metadata": {"new": True}}),
                        idempotency_key="run_homeostat_open:homeostat.open:duplicate",
                    )

                completed, _ = await service.complete_homeostat(
                    run_id=homeostat.run_id,
                    homeostat_id=homeostat.id,
                    values={"decision": "approved"},
                    idempotency_key="run_homeostat_open:homeostat.complete:homeostat_open_once",
                )
                self.assertEqual(completed.status, HomeostatStatus.completed)
                with self.assertRaisesRegex(ValueError, "already exists"):
                    await service.open_homeostat(
                        homeostat,
                        idempotency_key="run_homeostat_open:homeostat.open:after_complete",
                    )

        asyncio.run(scenario())

    def test_runtime_backend_service_completes_homeostat_idempotently(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                service = RuntimeBackendService.sqlite(Path(tmp_dir) / "fala.sqlite")
                await _put_test_run(service, "run_service")
                homeostat = Homeostat(
                    id="homeostat_review",
                    run_id="run_service",
                    impulse_id="impulse_review",
                    kind="human.review",
                    status=HomeostatStatus.open,
                )
                await service.save_homeostat(
                    homeostat,
                    idempotency_key="run_service:homeostat.save:homeostat_review",
                )

                completed, completion = await service.complete_homeostat(
                    run_id=homeostat.run_id,
                    homeostat_id=homeostat.id,
                    values={"decision": "approved"},
                    idempotency_key="run_service:homeostat.complete:homeostat_review",
                    actor="human:jan",
                )
                replayed, replay = await service.complete_homeostat(
                    run_id=homeostat.run_id,
                    homeostat_id=homeostat.id,
                    values={"decision": "rejected"},
                    idempotency_key="run_service:homeostat.complete:homeostat_review",
                    actor="human:jan",
                )

                self.assertFalse(completion.replayed)
                self.assertEqual(completed.status, HomeostatStatus.completed)
                self.assertEqual(completed.values, {"decision": "approved"})
                self.assertTrue(replay.replayed)
                self.assertEqual(replayed, completed)
                with self.assertRaisesRegex(ValueError, "not open"):
                    await service.complete_homeostat(
                        run_id=homeostat.run_id,
                        homeostat_id=homeostat.id,
                        values={"decision": "approved-again"},
                        idempotency_key="run_service:homeostat.complete:homeostat_review:again",
                    )
                events = await service.backend.list_events(run_id=homeostat.run_id)
                self.assertEqual(
                    [event.event_type for event in events],
                    ["homeostat.saved", "homeostat.completed"],
                )
                self.assertEqual(events[1].actor, "human:jan")
                self.assertEqual(events[1].payload["value_keys"], ["decision"])

        asyncio.run(scenario())

    def test_runtime_backend_service_cancels_and_expires_homeostats(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                service = RuntimeBackendService.sqlite(Path(tmp_dir) / "fala.sqlite")
                await _put_test_run(service, "run_homeostat_terminal")
                cancel_homeostat = Homeostat(
                    id="homeostat_cancel",
                    run_id="run_homeostat_terminal",
                    kind="human.review",
                    status=HomeostatStatus.open,
                )
                expire_homeostat = Homeostat(
                    id="homeostat_expire",
                    run_id="run_homeostat_terminal",
                    kind="human.review",
                    status=HomeostatStatus.open,
                )
                await service.open_homeostat(
                    cancel_homeostat,
                    idempotency_key="run_homeostat_terminal:homeostat.open:cancel",
                )
                await service.open_homeostat(
                    expire_homeostat,
                    idempotency_key="run_homeostat_terminal:homeostat.open:expire",
                )

                cancelled, cancel_submission = await service.cancel_homeostat(
                    run_id="run_homeostat_terminal",
                    homeostat_id=cancel_homeostat.id,
                    values={"reason": "operator"},
                    idempotency_key="run_homeostat_terminal:homeostat.cancel:cancel",
                    actor="cli:user",
                )
                replayed, replay = await service.cancel_homeostat(
                    run_id="run_homeostat_terminal",
                    homeostat_id=cancel_homeostat.id,
                    values={"reason": "changed"},
                    idempotency_key="run_homeostat_terminal:homeostat.cancel:cancel",
                    actor="cli:user",
                )
                expired, expire_submission = await service.expire_homeostat(
                    run_id="run_homeostat_terminal",
                    homeostat_id=expire_homeostat.id,
                    values={"reason": "timeout"},
                    idempotency_key="run_homeostat_terminal:homeostat.expire:expire",
                    actor="system",
                )

                self.assertEqual(cancelled.status, HomeostatStatus.cancelled)
                self.assertEqual(cancelled.values, {"reason": "operator"})
                self.assertFalse(cancel_submission.replayed)
                self.assertEqual(replayed, cancelled)
                self.assertTrue(replay.replayed)
                self.assertEqual(expired.status, HomeostatStatus.expired)
                self.assertEqual(expired.values, {"reason": "timeout"})
                self.assertFalse(expire_submission.replayed)
                with self.assertRaisesRegex(ValueError, "not open"):
                    await service.expire_homeostat(
                        run_id="run_homeostat_terminal",
                        homeostat_id=cancel_homeostat.id,
                        idempotency_key="run_homeostat_terminal:homeostat.expire:cancel",
                    )
                events = await service.backend.list_events(run_id="run_homeostat_terminal")
                self.assertEqual(
                    [event.event_type for event in events],
                    [
                        "homeostat.opened",
                        "homeostat.opened",
                        "homeostat.cancelled",
                        "homeostat.expired",
                    ],
                )
                self.assertEqual(events[2].actor, "cli:user")
                self.assertEqual(events[3].actor, "system")

        asyncio.run(scenario())

    def test_sqlite_backend_records_schema_migration(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "fala.sqlite"
            Correlator(db_path)
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

    def test_cli_db_init_status_and_migrate_manage_impulse_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "fala.sqlite"

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



    def test_cli_diagnoses_runtime_waits_and_deadlocks(self) -> None:
        async def scenario(db_path: Path) -> None:
            runtime = AutonomousCorrelator.sqlite(db_path)
            await runtime.create_run(
                Run(id="run_waits", title="Wait diagnostics"),
                idempotency_key="run_waits:create",
            )
            await runtime.service.backend.put_process(
                Process(
                    id="process_a",
                    run_id="run_waits",
                    process_type="join",
                    status=ProcessStatus.waiting,
                    input={"wait_for_processes": ["process_b"]},
                )
            )
            await runtime.service.backend.put_process(
                Process(
                    id="process_b",
                    run_id="run_waits",
                    process_type="review",
                    status=ProcessStatus.waiting,
                    input={
                        "wait_for_processes": ["process_a"],
                        "wait_for_homeostats": ["homeostat_review"],
                    },
                )
            )
            await runtime.service.backend.put_homeostat(
                Homeostat(
                    id="homeostat_review",
                    run_id="run_waits",
                    kind="manual_review",
                    status=HomeostatStatus.open,
                )
            )
            return await runtime.diagnose_waits(run_id="run_waits")

        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "fala.sqlite"
            direct = asyncio.run(scenario(db_path))
            self.assertTrue(direct.deadlocked)
            self.assertEqual(
                {frozenset(cycle) for cycle in direct.deadlocks},
                {frozenset({"process_a", "process_b"})},
            )

            cli = _run_cli_json("diagnose-waits", "--db", str(db_path), "--run-id", "run_waits")
            diagnostics = cli["wait_diagnostics"]
            self.assertTrue(diagnostics["deadlocked"])
            self.assertEqual(diagnostics["open_homeostats"], ["homeostat_review"])
            blocked = {
                item["process_id"]: item for item in diagnostics["blocked"]
            }
            self.assertIn("homeostat:homeostat_review", blocked["process_b"]["blocked_by"])

    def test_runtime_doctor_checks_sqlite_schema(self) -> None:
        async def scenario(db_path: Path) -> None:
            runtime = AutonomousCorrelator.sqlite(db_path)
            await runtime.create_run(
                Run(id="run_doctor", title="Doctor run"),
                idempotency_key="run_doctor:create",
            )
            await runtime.accept_impulse(
                Impulse(
                    id="impulse_doctor",
                    run_id="run_doctor",
                    impulse_type="case",
                ),
                idempotency_key="run_doctor:impulse.accept",
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
            self.assertEqual(doctor["counts"]["impulses"], 1)
            self.assertEqual(doctor["counts"]["runtime_events"], 2)

            package_path = (
                Path(__file__).resolve().parents[1]
                / "examples/correlation-paths/basic/fala-package.yaml"
            )
            doctor_with_package = _run_cli_json(
                "doctor",
                "--db",
                str(db_path),
                "--package",
                str(package_path),
            )
            self.assertTrue(doctor_with_package["ok"])
            self.assertEqual(doctor_with_package["packages"][0]["id"], "basic_examples")
            self.assertEqual(
                doctor_with_package["packages"][0]["correlation_path_count"],
                1,
            )

            bad_package = Path(tmp_dir) / "bad-package.yaml"
            bad_package.write_text(
                "version: '2'\nid: bad\nbogus_field: []\n",
                encoding="utf-8",
            )
            code, invalid_package = _run_cli_raw(
                "doctor",
                "--db",
                str(db_path),
                "--package",
                str(bad_package),
            )
            self.assertEqual(code, 1)
            self.assertFalse(invalid_package["ok"])
            self.assertFalse(invalid_package["packages"][0]["ok"])
            self.assertIn("bogus_field", invalid_package["packages"][0]["error"])

            missing_script_package = Path(tmp_dir) / "missing-script-package.yaml"
            missing_script_package.write_text(
                textwrap.dedent(
                    """
                    version: "2"
                    id: missing_script
                    capabilities:
                      - id: missing_capability
                    correlation_paths:
                      - id: basic
                        effectors:
                          - id: missing
                            capability: missing_capability
                            adapter:
                              kind: subprocess
                              command: ["python", "missing.py"]
                              cwd: "."
                    """
                ),
                encoding="utf-8",
            )
            code, adapter_invalid = _run_cli_raw(
                "doctor",
                "--db",
                str(db_path),
                "--package",
                str(missing_script_package),
            )
            self.assertEqual(code, 1)
            self.assertFalse(adapter_invalid["ok"])
            self.assertFalse(adapter_invalid["packages"][0]["ok"])
            self.assertIn(
                "missing.py",
                adapter_invalid["packages"][0]["adapter_errors"][0]["error"],
            )

            output = Path(tmp_dir) / "doctor.json"
            written = _run_cli_json(
                "doctor",
                "--db",
                str(db_path),
                "--package",
                str(package_path),
                "--output",
                str(output),
            )
            self.assertTrue(written["ok"])
            self.assertEqual(written["current_version"], SQLITE_RUNTIME_SCHEMA_VERSION)
            self.assertEqual(written["package_count"], 1)
            self.assertEqual(written["package_error_count"], 0)
            self.assertTrue(output.is_file())

    def test_sqlite_backend_persists_associations_homeostats_and_projections(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                backend = Correlator(Path(tmp_dir) / "fala.sqlite")
                await backend.put_run(Run(id="run_beta"))
                impulse = Impulse(
                    run_id="run_beta",
                    impulse_type="message",
                    payload={"text": "hello"},
                )
                await backend.put_impulse(impulse)

                association = Association(
                    run_id="run_beta",
                    impulse_id=impulse.id,
                    kind="classifier.score",
                    values={"score": 0.98},
                    metadata={"model": "local"},
                )
                await backend.put_association(association)

                homeostat = Homeostat(
                    run_id="run_beta",
                    impulse_id=impulse.id,
                    kind="human.approval",
                    status=HomeostatStatus.open,
                    values={"reason": "manual review"},
                )
                await backend.put_homeostat(homeostat)
                completed_homeostat = await backend.complete_homeostat(
                    run_id="run_beta",
                    homeostat_id=homeostat.id,
                    values={"approved": "yes"},
                )

                projection = Projection(
                    run_id="run_beta",
                    name="impulse_summary",
                    version=1,
                    data={"impulse_count": 1, "last_kind": association.kind},
                    source_event_sequence=0,
                )
                await backend.put_projection(projection)

                associations = await backend.list_associations(run_id="run_beta")
                stored_homeostat = await backend.get_homeostat(run_id="run_beta", homeostat_id=homeostat.id)
                stored_projection = await backend.get_projection(
                    run_id="run_beta", name="impulse_summary"
                )
                homeostats = await backend.list_homeostats(
                    run_id="run_beta",
                    status=HomeostatStatus.completed,
                )
                projections = await backend.list_projections(run_id="run_beta")

                self.assertEqual(associations, [association])
                self.assertEqual(stored_homeostat, completed_homeostat)
                self.assertEqual(stored_projection, projection)
                self.assertEqual(homeostats, [completed_homeostat])
                self.assertEqual(projections, [projection])

        asyncio.run(scenario())

    def test_runtime_backend_service_lists_runtime_systems(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                service = RuntimeBackendService.sqlite(Path(tmp_dir) / "fala.sqlite")
                await _put_test_run(service, "run_query")
                impulse = Impulse(
                    run_id="run_query",
                    impulse_type="message",
                    payload={"text": "hello"},
                )
                await service.accept_impulse(
                    impulse,
                    idempotency_key="run_query:impulse.accept:message",
                )
                association, _ = await service.record_association(
                    Association(
                        run_id="run_query",
                        impulse_id=impulse.id,
                        kind="classifier.score",
                        values={"score": 0.98},
                    ),
                    idempotency_key="run_query:association.record:score",
                )
                homeostat, _ = await service.save_homeostat(
                    Homeostat(
                        run_id="run_query",
                        impulse_id=impulse.id,
                        kind="human.approval",
                        status=HomeostatStatus.open,
                    ),
                    idempotency_key="run_query:homeostat.save:approval",
                )
                projection, _ = await service.save_projection(
                    Projection(
                        run_id="run_query",
                        name="impulse_summary",
                        data={"impulse_count": 1},
                        source_event_sequence=2,
                    ),
                    idempotency_key="run_query:projection.save:impulse_summary",
                )

                self.assertEqual(
                    await service.list_associations(run_id="run_query"),
                    [association],
                )
                self.assertEqual(
                    await service.list_homeostats(
                        run_id="run_query",
                        impulse_id=impulse.id,
                        status=HomeostatStatus.open,
                    ),
                    [homeostat],
                )
                self.assertEqual(
                    await service.list_projections(run_id="run_query"),
                    [projection],
                )

        asyncio.run(scenario())

    def test_sqlite_bridge_delivers_impulse_between_local_runtimes_idempotently(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                source_path = Path(tmp_dir) / "source.sqlite"
                target_path = Path(tmp_dir) / "target.sqlite"
                source = RuntimeBackendService.sqlite(source_path)
                target = RuntimeBackendService.sqlite(target_path)
                await _put_test_run(source, "run_source")
                await _put_test_run(target, "run_target")
                source_ref = RuntimeRef(id="source", uri=f"sqlite://{source_path}")
                target_ref = RuntimeRef(id="target", uri=f"sqlite://{target_path}")
                pool = RuntimePool(
                    id="local_pair",
                    runtimes=[source_ref, target_ref],
                    impulse_types=["case"],
                )
                policy = DelegationPolicy(
                    pool_id=pool.id,
                    impulse_types=["case"],
                    budget=RuntimeBudget(
                        runtime_hops=1,
                        spawned_runs=1,
                        impulse_count=1,
                        wall_time_seconds=30,
                        attempts=2,
                        reaction_bytes=4096,
                    ),
                )
                impulse = Impulse(
                    id="impulse_case",
                    run_id="run_source",
                    impulse_type="case",
                    payload={"claim": "CLM-1"},
                )

                await source.accept_impulse(
                    impulse,
                    idempotency_key="run_source:impulse.accept:impulse_case",
                )
                source_events = await source.backend.list_events(run_id="run_source")
                delivery = BridgeDelivery(
                    id="bridge_case",
                    run_id="run_source",
                    idempotency_key="run_source:bridge:case",
                    source=RunRef(runtime=source_ref, run_id="run_source"),
                    target=RunRef(runtime=target_ref, run_id="run_target"),
                    impulse=impulse,
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
                self.assertEqual(delivered.budget.impulse_count, 0)
                self.assertEqual(imported.budget.impulse_count, 0)
                self.assertFalse(delivered_submission.replayed)
                self.assertFalse(import_submission.replayed)
                self.assertEqual(replay_delivered, delivered)
                self.assertEqual(replay_imported, imported)
                self.assertTrue(delivered_replay.replayed)
                self.assertTrue(import_replay.replayed)

                target_impulse = await target.backend.get_impulse(
                    run_id="run_target",
                    impulse_id="impulse_case",
                )
                self.assertIsNotNone(target_impulse)
                assert target_impulse is not None
                self.assertEqual(target_impulse.run_id, "run_target")
                self.assertEqual(
                    target_impulse.metadata["source_runtime_id"],
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
                        "impulse.accepted",
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
                await _put_test_run(source, "run_source")
                await _put_test_run(target, "run_target")
                source_ref = RuntimeRef(id="source", uri=f"sqlite://{source_path}")
                target_ref = RuntimeRef(id="target", uri=f"sqlite://{target_path}")
                impulse = Impulse(
                    id="impulse_budget",
                    run_id="run_source",
                    impulse_type="case",
                )
                await source.accept_impulse(
                    impulse,
                    idempotency_key="run_source:impulse.accept:impulse_budget",
                )
                source_events = await source.backend.list_events(run_id="run_source")
                delivery = BridgeDelivery(
                    id="bridge_budget",
                    run_id="run_source",
                    idempotency_key="run_source:bridge:budget",
                    source=RunRef(runtime=source_ref, run_id="run_source"),
                    target=RunRef(runtime=target_ref, run_id="run_target"),
                    impulse=impulse,
                    event_ref=EventRef(
                        runtime=source_ref,
                        run_id="run_source",
                        event_id=source_events[0].id,
                        sequence=source_events[0].sequence,
                    ),
                    budget=RuntimeBudget(runtime_hops=1, impulse_count=1, attempts=1),
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

    def test_cli_delivers_bridge_between_local_runtimes(self) -> None:
        async def scenario(source_path: Path, target_path: Path) -> None:
            source = RuntimeBackendService.sqlite(source_path)
            target = RuntimeBackendService.sqlite(target_path)
            await _put_test_run(source, "run_source")
            await _put_test_run(target, "run_target")
            source_ref = RuntimeRef(id="source", uri=f"sqlite://{source_path}")
            target_ref = RuntimeRef(id="target", uri=f"sqlite://{target_path}")
            impulse = Impulse(
                id="impulse_cli_bridge",
                run_id="run_source",
                impulse_type="case",
                payload={"claim": "CLI-BRIDGE"},
            )
            await source.accept_impulse(
                impulse,
                idempotency_key="run_source:impulse.accept:impulse_cli_bridge",
            )
            source_events = await source.backend.list_events(run_id="run_source")
            await source.enqueue_bridge_delivery(
                BridgeDelivery(
                    id="bridge_cli",
                    run_id="run_source",
                    idempotency_key="run_source:bridge:cli",
                    source=RunRef(runtime=source_ref, run_id="run_source"),
                    target=RunRef(runtime=target_ref, run_id="run_target"),
                    impulse=impulse,
                    event_ref=EventRef(
                        runtime=source_ref,
                        run_id="run_source",
                        event_id=source_events[0].id,
                        sequence=source_events[0].sequence,
                    ),
                    budget=RuntimeBudget(runtime_hops=1, impulse_count=1),
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
                inbox["bridge_inbox"][0]["impulse"]["metadata"]["source_run_id"],
                "run_source",
            )

    def test_cli_exports_and_imports_bridge_delivery_file(self) -> None:
        async def scenario(source_path: Path, target_path: Path) -> None:
            source = RuntimeBackendService.sqlite(source_path)
            target = RuntimeBackendService.sqlite(target_path)
            await _put_test_run(source, "run_source")
            await _put_test_run(target, "run_target")
            source_ref = RuntimeRef(id="source", uri=f"sqlite://{source_path}")
            target_ref = RuntimeRef(id="target", uri=f"sqlite://{target_path}")
            impulse = Impulse(
                id="impulse_file_bridge",
                run_id="run_source",
                impulse_type="case",
                payload={"claim": "FILE-BRIDGE"},
            )
            await source.accept_impulse(
                impulse,
                idempotency_key="run_source:impulse.accept:impulse_file_bridge",
            )
            source_events = await source.backend.list_events(run_id="run_source")
            await source.enqueue_bridge_delivery(
                BridgeDelivery(
                    id="bridge_file",
                    run_id="run_source",
                    idempotency_key="run_source:bridge:file",
                    source=RunRef(runtime=source_ref, run_id="run_source"),
                    target=RunRef(runtime=target_ref, run_id="run_target"),
                    impulse=impulse,
                    event_ref=EventRef(
                        runtime=source_ref,
                        run_id="run_source",
                        event_id=source_events[0].id,
                        sequence=source_events[0].sequence,
                    ),
                    budget=RuntimeBudget(runtime_hops=1, impulse_count=1),
                ),
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            source_path = Path(tmp_dir) / "source.sqlite"
            target_path = Path(tmp_dir) / "target.sqlite"
            export_path = Path(tmp_dir) / "bridge_file.json"
            asyncio.run(scenario(source_path, target_path))

            exported = _run_cli_json(
                "bridge",
                "export",
                "--db",
                str(source_path),
                "--run-id",
                "run_source",
                "--delivery-id",
                "bridge_file",
                "--out",
                str(export_path),
            )
            self.assertTrue(exported["ok"])
            self.assertTrue(export_path.is_file())
            self.assertEqual(json.loads(export_path.read_text())["id"], "bridge_file")

            imported = _run_cli_json(
                "bridge",
                "import",
                "--db",
                str(target_path),
                "--file",
                str(export_path),
            )
            self.assertTrue(imported["ok"])
            self.assertEqual(imported["imported"]["run_id"], "run_target")
            self.assertFalse(imported["replayed"])

            replay = _run_cli_json(
                "bridge",
                "import",
                "--db",
                str(target_path),
                "--file",
                str(export_path),
            )
            self.assertTrue(replay["replayed"])

            inbox = _run_cli_json(
                "bridge",
                "list",
                "--db",
                str(target_path),
                "--run-id",
                "run_target",
                "--box",
                "inbox",
            )
            self.assertEqual(inbox["count"], 1)
            self.assertEqual(
                inbox["bridge_inbox"][0]["impulse"]["payload"]["claim"],
                "FILE-BRIDGE",
            )


if __name__ == "__main__":
    unittest.main()
