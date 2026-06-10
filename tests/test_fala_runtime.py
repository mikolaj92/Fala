from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import textwrap
import unittest
from datetime import datetime, timedelta, timezone
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import httpx

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pydantic import ValidationError

from fala.cli import main as runtime_cli_main
from fala.worker_cli import (
    _build_parser as build_runtime_worker_parser,
    main as runtime_worker_cli_main,
)
from fala import (
    AdapterProcessRuntimeWorker,
    AdapterRegistry,
    AdapterSpec,
    ArtifactRef,
    ClaimedProcess,
    CombineSpec,
    ExternalCommandAdapter,
    InMemoryStateStore,
    PipelineSpec,
    PipelineRegistry,
    PipelineRegistryError,
    PipelineRunError,
    PipelineRunner,
    PipelineScheduler,
    ProcessAction,
    ProcessEvent,
    ProcessExecutionContext,
    ProcessInput,
    ProcessOutput,
    ProcessRuntimeClient,
    ProcessSpec,
    ProcessStatus,
    ProcessWorkerResult,
    RetryPolicy,
    ScheduledProcess,
    SQLiteStateStore,
    WorkflowPackageSpec,
    WorkflowWorkerSpec,
    load_pipeline_yaml,
)
from fala.state import build_runtime_document_state, build_runtime_state


class _FakeRuntimeClient:
    def __init__(self, claim: ClaimedProcess | None) -> None:
        self.claim = claim
        self.outputs: list[dict] = []
        self.statuses: list[dict] = []
        self.renews: list[dict] = []
        self.events: list[dict] = []

    async def claim_next(self, **kwargs) -> ClaimedProcess | None:
        self.claim_args = kwargs
        claim = self.claim
        self.claim = None
        return claim

    async def write_output(self, **kwargs) -> dict:
        self.outputs.append(kwargs)
        return {"ok": True}

    async def write_status(self, **kwargs) -> None:
        self.statuses.append(kwargs)

    async def append_event(self, **kwargs):
        self.events.append(kwargs)
        return kwargs["event"]

    async def renew_claim(self, **kwargs):
        self.renews.append(kwargs)
        return object()


def _run_cli(*args: str) -> dict:
    buffer = StringIO()
    with redirect_stdout(buffer):
        code = runtime_cli_main(list(args))
    if code != 0:
        raise AssertionError(buffer.getvalue())
    return json.loads(buffer.getvalue())


def _run_cli_raw(*args: str) -> tuple[int, dict]:
    buffer = StringIO()
    with redirect_stdout(buffer):
        code = runtime_cli_main(list(args))
    return code, json.loads(buffer.getvalue())


class ProcessRuntimeTests(unittest.TestCase):
    def test_process_runtime_package_stays_domain_agnostic(self) -> None:
        root = SRC_DIR / "fala"
        forbidden = [
            "rudy",
            "msds",
            "sds",
            "pdf_to_md",
            "pdf_path",
            "processed_pdfs",
            "total_pdfs",
            "llm_extract",
            "reference_enrichment",
            "regulations",
        ]
        for path in root.glob("*.py"):
            text = path.read_text(encoding="utf-8").lower()
            for token in forbidden:
                with self.subTest(path=path.name, token=token):
                    self.assertNotIn(token, text)

    def test_example_pipeline_uses_separate_step_programs(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        pipeline = load_pipeline_yaml(
            repo_root
            / "examples"
            / "pipelines"
            / "basic"
            / "basic_enrichment.yaml"
        )
        commands = []
        for step in pipeline.steps:
            if step.adapter.kind != "subprocess":
                continue
            command = step.adapter.command or []
            with self.subTest(step=step.id):
                self.assertEqual(command, ["python", f"steps/{step.id}.py"])
            commands.append(tuple(command))

        self.assertEqual(len(commands), len(set(commands)))

    def test_pipeline_registry_loads_workflow_package_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "demo_package"
            package_dir.mkdir()
            (package_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: demo_package
                    title: Demo workflow package
                    description: Demo package
                    tags: ["demo"]
                    version: "1"
                    pipelines:
                      - demo.yaml
                    workers:
                      - id: first_worker
                        pipeline: packaged_demo
                        process: first
                        adapter_kind: queue
                        command: ["python", "steps/first.py"]
                        cwd: "."
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "demo.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: packaged_demo
                    steps:
                      - id: first
                        adapter:
                          kind: queue
                          queue: demo.first
                    """
                ).strip(),
                encoding="utf-8",
            )

            registry = PipelineRegistry.from_directory(root)
            self.assertEqual(registry.get("packaged_demo").id, "packaged_demo")
            self.assertEqual(
                registry.pipeline_package_id("packaged_demo"),
                "demo_package",
            )
            self.assertEqual(registry.packages()[0].id, "demo_package")
            self.assertEqual(registry.packages()[0].workers[0].id, "first_worker")
            self.assertEqual(
                registry.packages()[0].workers[0].pipeline_id,
                "packaged_demo",
            )
            self.assertEqual(registry.packages()[0].workers[0].process_id, "first")
            self.assertEqual(
                Path(registry.packages()[0].workers[0].cwd or "").name,
                "demo_package",
            )
            self.assertTrue(
                registry.pipeline_source("packaged_demo").endswith("demo.yaml")
            )

            validate = _run_cli(
                "--pipeline-dir",
                str(root),
                "validate",
                "--json",
            )
            self.assertEqual(validate["package_count"], 1)
            self.assertEqual(validate["packages"][0]["id"], "demo_package")
            self.assertEqual(validate["packages"][0]["workers"][0]["id"], "first_worker")
            self.assertEqual(validate["pipelines"][0]["package_id"], "demo_package")

            commands = _run_cli(
                "--pipeline-dir",
                str(root),
                "worker-commands",
                "--base-url",
                "http://runtime.test",
                "--run-id",
                "run_manifest",
                "--package-id",
                "demo_package",
            )
            self.assertTrue(commands["ok"])
            self.assertEqual(commands["workers"][0]["worker_id"], "first_worker")
            self.assertEqual(commands["workers"][0]["pipeline_id"], "packaged_demo")
            self.assertEqual(commands["workers"][0]["process_id"], "first")
            self.assertIn("--package-worker", commands["workers"][0]["argv"])
            self.assertIn("first_worker", commands["workers"][0]["shell"])

    def test_pipeline_registry_rejects_invalid_package_worker_references(self) -> None:
        cases = [
            (
                """
                workers:
                  - id: first_worker
                    pipeline: missing_flow
                    process: first
                    command: ["python", "steps/first.py"]
                """,
                """
                pipeline: packaged_demo
                steps:
                  - id: first
                    adapter:
                      kind: queue
                      queue: demo.first
                """,
                "outside the package",
            ),
            (
                """
                workers:
                  - id: first_worker
                    pipeline: packaged_demo
                    process: missing
                    command: ["python", "steps/first.py"]
                """,
                """
                pipeline: packaged_demo
                steps:
                  - id: first
                    adapter:
                      kind: queue
                      queue: demo.first
                """,
                "unknown process",
            ),
            (
                """
                workers:
                  - id: first_worker
                    pipeline: packaged_demo
                    process: first
                    command: ["python", "steps/first.py"]
                """,
                """
                pipeline: packaged_demo
                steps:
                  - id: first
                    adapter:
                      kind: subprocess
                      command: ["python", "steps/first.py"]
                """,
                "adapter kind",
            ),
        ]
        for worker_yaml, pipeline_yaml, expected_error in cases:
            with self.subTest(expected_error=expected_error):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    package_dir = root / "demo_package"
                    package_dir.mkdir()
                    (package_dir / "process-runtime-package.yaml").write_text(
                        (
                            textwrap.dedent(
                                """
                            package: demo_package
                            pipelines:
                              - demo.yaml
                            """
                            ).strip()
                            + "\n"
                            + textwrap.dedent(worker_yaml).strip()
                            + "\n"
                        ),
                        encoding="utf-8",
                    )
                    (package_dir / "demo.yaml").write_text(
                        textwrap.dedent(pipeline_yaml).strip(),
                        encoding="utf-8",
                    )

                    with self.assertRaisesRegex(PipelineRegistryError, expected_error):
                        PipelineRegistry.from_directory(root)

    def test_runtime_ids_reject_path_and_whitespace_values(self) -> None:
        valid = PipelineSpec(
            id="workflow.alpha-1",
            input_values={"document_path": "{path}", "document_uri": "{uri}"},
            steps=[
                ProcessSpec(
                    id="extract_text",
                    adapter=AdapterSpec(kind="queue", queue="workflow.extract"),
                )
            ],
            combines=[CombineSpec(id="combined.latest", needs=["extract_text"])],
        )
        self.assertEqual(valid.id, "workflow.alpha-1")
        self.assertEqual(valid.input_values["document_path"], "{path}")

        invalid_models = [
            lambda: ArtifactRef(id="bad/artifact", kind="json", uri="file:///tmp/out.json"),
            lambda: ProcessSpec(id="bad step", adapter=AdapterSpec(kind="queue", queue="q")),
            lambda: CombineSpec(id="1starts_with_digit", needs=[]),
            lambda: PipelineSpec(
                id="bad/workflow",
                steps=[ProcessSpec(id="first", adapter=AdapterSpec(kind="queue", queue="q"))],
            ),
            lambda: WorkflowPackageSpec(id="workflow_package", pipelines=["/tmp/demo.yaml"]),
            lambda: WorkflowPackageSpec(id="workflow_package", pipelines=["../demo.yaml"]),
            lambda: WorkflowPackageSpec(
                id="workflow_package",
                pipelines=["demo.yaml"],
                workers=[
                    WorkflowWorkerSpec(
                        id="worker",
                        pipeline_id="workflow",
                        adapter_kind="subprocess",
                        command=["worker"],
                    )
                ],
            ),
            lambda: ProcessExecutionContext(
                pipeline_id="workflow",
                run_id="run_1",
                document_id="folder/doc.pdf",
                process_id="bad/process",
                attempt=1,
                input=ProcessInput(),
            ),
        ]
        for factory in invalid_models:
            with self.subTest(factory=factory):
                with self.assertRaises(ValidationError):
                    factory()

    def test_process_actions_are_typed_runtime_contract(self) -> None:
        self.assertEqual(ProcessAction("retry"), ProcessAction.retry)
        with self.assertRaises(ValueError):
            ProcessAction("restart")

    def test_adapter_spec_requires_exact_boundary_shape(self) -> None:
        self.assertEqual(
            AdapterSpec(kind="subprocess", command=["worker"]).command,
            ["worker"],
        )
        self.assertEqual(AdapterSpec(kind="http", url="http://worker.local").url, "http://worker.local")
        self.assertEqual(AdapterSpec(kind="queue", queue="workflow.extract").queue, "workflow.extract")

        invalid_specs = [
            {"kind": "subprocess"},
            {"kind": "subprocess", "command": [], "queue": "workflow.extract"},
            {"kind": "http"},
            {"kind": "http", "url": "http://worker.local", "env": {"A": "B"}},
            {"kind": "queue"},
            {"kind": "queue", "queue": "workflow.extract", "command": ["worker"]},
        ]
        for spec in invalid_specs:
            with self.subTest(spec=spec):
                with self.assertRaises(ValidationError):
                    AdapterSpec.model_validate(spec)

    def test_runtime_state_builder_returns_typed_pipeline_order_snapshot(self) -> None:
        pipeline = PipelineSpec(
            id="demo",
            steps=[
                ProcessSpec(
                    id="first",
                    adapter=AdapterSpec(kind="queue", queue="demo.first"),
                ),
                ProcessSpec(
                    id="second",
                    needs=["first"],
                    adapter=AdapterSpec(kind="http", url="http://worker.local/second"),
                ),
            ],
        )
        output = ProcessOutput(
            values={"text": "ok"},
            artifacts=[ArtifactRef(kind="text", uri="file:///tmp/first.txt")],
        )

        document = build_runtime_document_state(
            document_id="doc.pdf",
            pipeline_id="demo",
            pipeline=pipeline,
            statuses={
                "first": ProcessStatus.completed,
                "second": ProcessStatus.queued,
            },
            claims={},
            outputs={"first": output},
            projections={},
            events=[],
        )
        state = build_runtime_state(run_id="run_1", documents=[document])
        payload = state.model_dump(mode="json")

        self.assertEqual([step.id for step in document.steps], ["first", "second"])
        self.assertEqual(document.steps[0].status, ProcessStatus.completed)
        self.assertEqual(document.steps[0].output_value_keys, ["text"])
        self.assertEqual(document.steps[0].artifact_count, 1)
        self.assertEqual(document.steps[1].needs, ["first"])
        self.assertEqual(document.steps[1].adapter_kind, "http")
        self.assertEqual(state.summary.document_count, 1)
        self.assertEqual(state.summary.process_count, 2)
        self.assertEqual(state.summary.status_counts["completed"], 1)
        self.assertEqual(state.summary.status_counts["queued"], 1)
        self.assertEqual(state.summary.pipeline_counts["demo"], 1)
        self.assertEqual(state.summary.output_count, 1)
        self.assertEqual(state.summary.artifact_count, 1)
        self.assertEqual(payload["documents"][0]["steps"][0]["status"], "completed")
        self.assertEqual(payload["summary"]["process_count"], 2)

    def test_runtime_cli_validates_initializes_and_claims_from_sqlite(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipeline_dir = root / "pipelines"
            pipeline_dir.mkdir()
            (pipeline_dir / "demo.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: demo
                    steps:
                      - id: first
                        adapter:
                          kind: queue
                          queue: demo.first
                      - id: second
                        needs: [first]
                        adapter:
                          kind: queue
                          queue: demo.second
                    combines:
                      - id: bundle
                        needs: [first, second]
                    """
                ),
                encoding="utf-8",
            )
            db_path = root / "runtime.db"

            validate = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "validate",
                "--json",
            )
            self.assertEqual(validate["pipeline_count"], 1)
            self.assertEqual(validate["pipelines"][0]["id"], "demo")

            init = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "init-document",
                "--db",
                str(db_path),
                "--pipeline",
                "demo",
                "--run-id",
                "run_cli",
                "--document-id",
                "folder/doc.pdf",
                "--value",
                "source=doc.pdf",
                "--artifact",
                "pdf=file:///tmp/doc.pdf",
            )
            self.assertEqual([item["id"] for item in init["schedule"]["queued"]], ["first"])
            self.assertEqual(init["schedule"]["waiting"], ["second"])

            claim = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "claim",
                "--db",
                str(db_path),
                "--pipeline",
                "demo",
                "--run-id",
                "run_cli",
                "--worker-id",
                "worker-cli",
                "--adapter-kind",
                "queue",
            )
            self.assertEqual(claim["claim"]["process"]["id"], "first")
            self.assertEqual(claim["claim"]["worker_id"], "worker-cli")
            self.assertEqual(
                claim["claim"]["context"]["input"]["values"]["initial"]["source"],
                "doc.pdf",
            )

    def test_runtime_cli_scaffolds_executable_workflow_package(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "demo_package"
            scaffold = _run_cli(
                "scaffold",
                "--output-dir",
                str(package_dir),
                "--package-id",
                "demo_package",
                "--pipeline-id",
                "demo_flow",
                "--steps",
                "ingest,enrich,export",
            )
            self.assertEqual(scaffold["package_id"], "demo_package")
            self.assertEqual(scaffold["pipeline_id"], "demo_flow")
            self.assertEqual(scaffold["step_ids"], ["ingest", "enrich", "export"])
            self.assertTrue((package_dir / "process-runtime-package.yaml").exists())
            self.assertTrue((package_dir / "demo_flow.yaml").exists())
            self.assertTrue((package_dir / "steps" / "ingest.py").exists())

            validate = _run_cli(
                "--pipeline-dir",
                str(root),
                "validate",
                "--json",
                "--check-commands",
            )
            self.assertTrue(validate["ok"])
            self.assertEqual(validate["packages"][0]["id"], "demo_package")
            self.assertEqual(validate["pipelines"][0]["steps"][0]["id"], "ingest")

            db_path = root / "runtime.db"
            init = _run_cli(
                "--pipeline-dir",
                str(root),
                "init-document",
                "--db",
                str(db_path),
                "--pipeline",
                "demo_flow",
                "--run-id",
                "run_scaffold",
                "--document-id",
                "doc.txt",
                "--value",
                "source=doc.txt",
            )
            self.assertEqual([item["id"] for item in init["schedule"]["queued"]], ["ingest"])

            worked = _run_cli(
                "--pipeline-dir",
                str(root),
                "run-until-idle",
                "--db",
                str(db_path),
                "--pipeline",
                "demo_flow",
                "--run-id",
                "run_scaffold",
                "--worker-id",
                "worker-scaffold",
                "--adapter-kind",
                "subprocess",
            )
            self.assertEqual(worked["completed_count"], 3)
            document = worked["state"]["documents"][0]
            self.assertEqual(document["statuses"]["ingest"], "completed")
            self.assertEqual(document["statuses"]["enrich"], "completed")
            self.assertEqual(document["statuses"]["export"], "completed")
            self.assertTrue(document["projections"]["workflow_result"]["complete"])

    def test_runtime_cli_scaffolds_queue_workflow_package(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "queue_package"
            scaffold = _run_cli(
                "scaffold",
                "--output-dir",
                str(package_dir),
                "--package-id",
                "queue_package",
                "--pipeline-id",
                "queue_flow",
                "--steps",
                "ingest,enrich,export",
                "--adapter-kind",
                "queue",
            )
            self.assertEqual(scaffold["adapter_kind"], "queue")
            self.assertTrue((package_dir / "steps" / "enrich.py").exists())
            package_text = (package_dir / "process-runtime-package.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn("workers:", package_text)
            self.assertIn("id: ingest_worker", package_text)
            self.assertIn("pipeline: queue_flow", package_text)
            self.assertIn("process: ingest", package_text)
            self.assertIn('command: ["python", "steps/ingest.py"]', package_text)
            pipeline_text = (package_dir / "queue_flow.yaml").read_text(encoding="utf-8")
            self.assertIn("kind: queue", pipeline_text)
            self.assertIn('queue: "queue_package.ingest"', pipeline_text)
            self.assertNotIn("command:", pipeline_text)

            validate = _run_cli(
                "--pipeline-dir",
                str(root),
                "validate",
                "--json",
                "--check-commands",
            )
            self.assertTrue(validate["ok"])
            self.assertEqual(validate["pipelines"][0]["steps"][0]["adapter_kind"], "queue")
            self.assertEqual(
                validate["packages"][0]["workers"][0]["process_id"],
                "ingest",
            )

            db_path = root / "runtime.db"
            init = _run_cli(
                "--pipeline-dir",
                str(root),
                "init-document",
                "--db",
                str(db_path),
                "--pipeline",
                "queue_flow",
                "--run-id",
                "run_queue_scaffold",
                "--document-id",
                "doc.txt",
                "--value",
                "source=doc.txt",
            )
            self.assertEqual([item["id"] for item in init["schedule"]["queued"]], ["ingest"])

            claim = _run_cli(
                "--pipeline-dir",
                str(root),
                "claim",
                "--db",
                str(db_path),
                "--pipeline",
                "queue_flow",
                "--run-id",
                "run_queue_scaffold",
                "--worker-id",
                "queue-worker",
                "--adapter-kind",
                "queue",
            )
            self.assertEqual(claim["claim"]["process"]["id"], "ingest")
            self.assertEqual(claim["claim"]["process"]["adapter"]["kind"], "queue")
            self.assertEqual(
                claim["claim"]["process"]["adapter"]["queue"],
                "queue_package.ingest",
            )

    def test_runtime_cli_optionally_validates_subprocess_commands(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipeline_dir = root / "pipelines"
            pipeline_dir.mkdir()
            (pipeline_dir / "broken.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: broken_commands
                    steps:
                      - id: missing_tool
                        adapter:
                          kind: subprocess
                          command: ["definitely-missing-fala-command"]
                    """
                ),
                encoding="utf-8",
            )

            schema_only = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "validate",
                "--json",
            )
            self.assertTrue(schema_only["ok"])

            code, checked = _run_cli_raw(
                "--pipeline-dir",
                str(pipeline_dir),
                "validate",
                "--json",
                "--check-commands",
            )

        self.assertEqual(code, 1)
        self.assertFalse(checked["ok"])
        self.assertEqual(checked["command_issues"][0]["pipeline_id"], "broken_commands")
        self.assertEqual(checked["command_issues"][0]["process_id"], "missing_tool")
        self.assertIn("not found on PATH", checked["command_issues"][0]["reason"])

    def test_runtime_cli_validates_script_files_in_commands(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipeline_dir = root / "pipelines"
            pipeline_dir.mkdir()
            (pipeline_dir / "missing_script.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: missing_script
                    steps:
                      - id: extract
                        adapter:
                          kind: subprocess
                          command: ["python", "steps/missing.py"]
                          cwd: "."
                    """
                ),
                encoding="utf-8",
            )
            package_dir = pipeline_dir / "package"
            package_dir.mkdir()
            (package_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: package
                    pipelines:
                      - packaged.yaml
                    workers:
                      - id: enrich_worker
                        pipeline: packaged
                        process: enrich
                        command: ["python", "steps/missing_worker.py"]
                        cwd: "."
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "packaged.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: packaged
                    steps:
                      - id: enrich
                        adapter:
                          kind: queue
                          queue: package.enrich
                    """
                ).strip(),
                encoding="utf-8",
            )

            schema_only = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "validate",
                "--json",
            )
            self.assertTrue(schema_only["ok"])

            code, checked = _run_cli_raw(
                "--pipeline-dir",
                str(pipeline_dir),
                "validate",
                "--json",
                "--check-commands",
            )

        self.assertEqual(code, 1)
        self.assertFalse(checked["ok"])
        reasons = {
            (issue.get("pipeline_id"), issue.get("process_id"), issue.get("worker_id")): issue["reason"]
            for issue in checked["command_issues"]
        }
        self.assertIn(
            "command file path does not exist",
            reasons[("missing_script", "extract", None)],
        )
        self.assertIn(
            "command file path does not exist",
            reasons[("packaged", "enrich", "enrich_worker")],
        )

    def test_runtime_cli_exports_schema_and_validates_contract_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            output_path = root / "output.json"
            context_path = root / "context.json"
            invalid_path = root / "invalid.json"
            output_path.write_text(
                json.dumps(
                    {
                        "values": {"status": "ok"},
                        "artifacts": [
                            {
                                "kind": "json",
                                "uri": "file:///tmp/result.json",
                            }
                        ],
                        "metadata": {"source": "unit-test"},
                    }
                ),
                encoding="utf-8",
            )
            context_path.write_text(
                json.dumps(
                    {
                        "pipeline_id": "demo",
                        "run_id": "run_contract",
                        "document_id": "doc_1",
                        "process_id": "step_1",
                        "attempt": 1,
                        "input": {
                            "artifacts": [],
                            "values": {
                                "initial": {"source": "input.txt"},
                                "needs": {"previous": {"ok": True}},
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            invalid_path.write_text(json.dumps({"artifacts": "not-a-list"}), encoding="utf-8")

            schema = _run_cli("schema", "process-output")
            self.assertEqual(schema["model"], "process-output")
            self.assertIn("values", schema["schema"]["properties"])
            self.assertIn("artifacts", schema["schema"]["properties"])
            pipeline_schema = _run_cli("schema", "pipeline")
            self.assertIn("input_values", pipeline_schema["schema"]["properties"])
            package_schema = _run_cli("schema", "workflow-package")
            self.assertEqual(package_schema["model"], "workflow-package")
            self.assertIn("pipelines", package_schema["schema"]["properties"])
            self.assertIn("workers", package_schema["schema"]["properties"])
            artifact_schema = _run_cli("schema", "artifact")
            self.assertIn("pattern", artifact_schema["schema"]["properties"]["id"])
            action_schema = _run_cli("schema", "process-action")
            self.assertEqual(action_schema["model"], "process-action")
            self.assertIn("action", action_schema["schema"]["properties"])
            self.assertIn("reason", action_schema["schema"]["properties"])
            event_page_schema = _run_cli("schema", "event-page")
            self.assertEqual(event_page_schema["model"], "event-page")
            self.assertIn("events", event_page_schema["schema"]["properties"])
            self.assertIn("next_after_event_id", event_page_schema["schema"]["properties"])
            runtime_state_schema = _run_cli("schema", "runtime-state")
            self.assertEqual(runtime_state_schema["model"], "runtime-state")
            self.assertIn("summary", runtime_state_schema["schema"]["properties"])
            self.assertIn("documents", runtime_state_schema["schema"]["properties"])

            output = _run_cli("validate-output", "--file", str(output_path))
            self.assertTrue(output["ok"])
            self.assertEqual(output["artifact_count"], 1)
            self.assertEqual(output["value_keys"], ["status"])
            self.assertEqual(output["metadata_keys"], ["source"])

            context = _run_cli("validate-context", "--file", str(context_path))
            self.assertTrue(context["ok"])
            self.assertEqual(context["pipeline_id"], "demo")
            self.assertEqual(context["initial_keys"], ["source"])
            self.assertEqual(context["needs"], ["previous"])

            code, invalid = _run_cli_raw("validate-output", "--file", str(invalid_path))
            self.assertEqual(code, 1)
            self.assertFalse(invalid["ok"])
            self.assertIn("ProcessOutput validation failed", invalid["error"])

    def test_runtime_cli_work_once_runs_subprocess_and_updates_sqlite(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            worker_script = root / "external_worker.py"
            worker_script.write_text(
                textwrap.dedent(
                    """
                    import json
                    import os
                    import sys

                    ctx = json.loads(sys.stdin.read())
                    print(
                        'PROCESS_RUNTIME_EVENT {"type":"process.progress","status":"running","data":{"stage":"external"}}',
                        file=sys.stderr,
                    )
                    print(json.dumps({
                        "values": {
                            "document": ctx["document_id"],
                            "source": ctx["input"]["values"]["initial"]["source"],
                            "env_worker_process": os.environ["PROCESS_RUNTIME_PROCESS_ID"],
                            "artifact_dir_exists": os.path.isdir(os.environ["PROCESS_RUNTIME_ARTIFACT_DIR"]),
                        }
                    }))
                    """
                ),
                encoding="utf-8",
            )
            pipeline_dir = root / "pipelines"
            pipeline_dir.mkdir()
            (pipeline_dir / "demo.yaml").write_text(
                textwrap.dedent(
                    f"""
                    pipeline: demo
                    steps:
                      - id: external
                        adapter:
                          kind: subprocess
                          command:
                            - {json.dumps(sys.executable)}
                            - {json.dumps(str(worker_script))}
                          env:
                            PROCESS_RUNTIME_ARTIFACT_ROOT: {json.dumps(str(root / "artifacts"))}
                      - id: downstream
                        needs: [external]
                        adapter:
                          kind: queue
                          queue: demo.downstream
                    combines:
                      - id: bundle
                        needs: [external]
                    """
                ),
                encoding="utf-8",
            )
            db_path = root / "runtime.db"

            _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "init-document",
                "--db",
                str(db_path),
                "--pipeline",
                "demo",
                "--run-id",
                "run_cli",
                "--document-id",
                "doc.pdf",
                "--value",
                "source=doc.pdf",
            )
            worked = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "work-once",
                "--db",
                str(db_path),
                "--pipeline",
                "demo",
                "--run-id",
                "run_cli",
                "--worker-id",
                "worker-cli",
                "--adapter-kind",
                "subprocess",
            )

            self.assertTrue(worked["completed"])
            self.assertEqual(worked["claim"]["process"]["id"], "external")
            self.assertEqual(worked["output"]["values"]["source"], "doc.pdf")
            self.assertEqual(worked["schedule"]["completed"], ["external"])
            self.assertEqual(
                [item["id"] for item in worked["schedule"]["queued"]],
                ["downstream"],
            )
            self.assertTrue(worked["refreshed_projections"][0]["complete"])
            status = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "status",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--include-events",
            )
            events = status["state"]["documents"][0]["events"]
            self.assertIn("process.progress", [event["type"] for event in events])
            progress = next(event for event in events if event["type"] == "process.progress")
            self.assertEqual(progress["data"]["stage"], "external")

    def test_runtime_cli_run_until_idle_executes_full_subprocess_workflow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            worker_script = root / "step_worker.py"
            worker_script.write_text(
                textwrap.dedent(
                    """
                    import json
                    import sys

                    process_id = sys.argv[1]
                    ctx = json.loads(sys.stdin.read())
                    initial = ctx["input"]["values"]["initial"]
                    needs = ctx["input"]["values"]["needs"]

                    if process_id == "first":
                        values = {"number": int(initial["number"])}
                    elif process_id == "double":
                        values = {"number": needs["first"]["number"] * 2}
                    elif process_id == "label":
                        values = {"label": f"value:{needs['double']['number']}"}
                    else:
                        raise SystemExit(f"unknown process {process_id}")

                    print(json.dumps({"values": values}))
                    """
                ),
                encoding="utf-8",
            )
            pipeline_dir = root / "pipelines"
            pipeline_dir.mkdir()
            (pipeline_dir / "demo.yaml").write_text(
                textwrap.dedent(
                    f"""
                    pipeline: demo
                    steps:
                      - id: first
                        adapter:
                          kind: subprocess
                          command:
                            - {json.dumps(sys.executable)}
                            - {json.dumps(str(worker_script))}
                            - first
                          env:
                            PROCESS_RUNTIME_ARTIFACT_ROOT: {json.dumps(str(root / "artifacts"))}
                      - id: double
                        needs: [first]
                        adapter:
                          kind: subprocess
                          command:
                            - {json.dumps(sys.executable)}
                            - {json.dumps(str(worker_script))}
                            - double
                          env:
                            PROCESS_RUNTIME_ARTIFACT_ROOT: {json.dumps(str(root / "artifacts"))}
                      - id: label
                        needs: [double]
                        adapter:
                          kind: subprocess
                          command:
                            - {json.dumps(sys.executable)}
                            - {json.dumps(str(worker_script))}
                            - label
                          env:
                            PROCESS_RUNTIME_ARTIFACT_ROOT: {json.dumps(str(root / "artifacts"))}
                    combines:
                      - id: final_view
                        needs: [first, double, label]
                    """
                ),
                encoding="utf-8",
            )
            db_path = root / "runtime.db"

            _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "init-document",
                "--db",
                str(db_path),
                "--pipeline",
                "demo",
                "--run-id",
                "run_cli",
                "--document-id",
                "doc-1",
                "--value",
                "number=21",
            )
            run = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "run-until-idle",
                "--db",
                str(db_path),
                "--pipeline",
                "demo",
                "--run-id",
                "run_cli",
                "--worker-id",
                "operator",
                "--adapter-kind",
                "subprocess",
            )

            self.assertTrue(run["idle"])
            self.assertEqual(run["completed_count"], 3)
            self.assertEqual(
                [step["process_id"] for step in run["steps"]],
                ["first", "double", "label"],
            )
            state = run["state"]["documents"][0]
            self.assertEqual(state["statuses"]["first"], "completed")
            self.assertEqual(state["statuses"]["double"], "completed")
            self.assertEqual(state["statuses"]["label"], "completed")
            self.assertEqual(
                [step["id"] for step in state["steps"]],
                ["first", "double", "label"],
            )
            self.assertEqual(state["steps"][1]["needs"], ["first"])
            self.assertEqual(state["steps"][1]["adapter_kind"], "subprocess")
            self.assertTrue(state["steps"][2]["has_output"])
            self.assertEqual(state["steps"][2]["output_value_keys"], ["label"])
            self.assertEqual(state["outputs"]["label"]["values"]["label"], "value:42")
            self.assertTrue(state["projections"]["final_view"]["complete"])
            self.assertGreater(state["event_count"], 0)
            self.assertEqual(state["events"], [])

            status = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "status",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
            )
            self.assertEqual(
                status["state"]["documents"][0]["outputs"]["double"]["values"]["number"],
                42,
            )
            self.assertEqual(
                [step["id"] for step in status["state"]["documents"][0]["steps"]],
                ["first", "double", "label"],
            )
            self.assertEqual(status["state"]["documents"][0]["events"], [])

            status_with_events = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "status",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--include-events",
            )
            self.assertGreater(
                len(status_with_events["state"]["documents"][0]["events"]),
                0,
            )

    def test_runtime_http_client_uses_framework_api_contract(self) -> None:
        claim = ClaimedProcess(
            pipeline_id="pipeline",
            run_id="run_http",
            document_id="folder/doc.pdf",
            process=ScheduledProcess(id="extract", needs=[], adapter={"kind": "queue"}),
            worker_id="worker-http",
            attempt=1,
            claim_expires_at=datetime.now(timezone.utc) + timedelta(seconds=60),
            context=ProcessExecutionContext(
                pipeline_id="pipeline",
                run_id="run_http",
                document_id="folder/doc.pdf",
                process_id="extract",
                attempt=1,
                input=ProcessInput(values={"initial": {"source": "doc.pdf"}, "needs": {}}),
            ),
        )
        requests: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if request.url.path.endswith("/packages/vendor_review"):
                return httpx.Response(
                    200,
                    json={
                        "package": {
                            "id": "vendor_review",
                            "title": "Vendor review",
                            "description": None,
                            "tags": [],
                            "version": "1",
                            "pipelines": ["review.yaml"],
                            "pipeline_ids": ["pipeline"],
                        },
                        "pipelines": [{"id": "pipeline", "steps": []}],
                    },
                )
            if request.url.path.endswith("/packages"):
                return httpx.Response(
                    200,
                    json={
                        "packages": [
                            {
                                "id": "vendor_review",
                                "title": "Vendor review",
                                "description": None,
                                "tags": [],
                                "version": "1",
                                "pipelines": ["review.yaml"],
                                "pipeline_ids": ["pipeline"],
                            }
                        ]
                    },
                )
            if request.url.path.endswith("/pipelines/pipeline"):
                return httpx.Response(
                    200,
                    json={
                        "package_id": "vendor_review",
                        "pipeline": {"id": "pipeline", "steps": []},
                    },
                )
            if request.url.path.endswith("/pipelines"):
                return httpx.Response(
                    200,
                    json={
                        "packages": [],
                        "pipelines": [{"id": "pipeline", "package_id": "vendor_review"}],
                    },
                )
            if request.url.path.endswith("/process-runtime") and request.method == "GET":
                return httpx.Response(
                    200,
                    json={
                        "run_id": "run_http",
                        "summary": {
                            "document_count": 1,
                            "process_count": 0,
                            "status_counts": {},
                            "pipeline_counts": {"pipeline": 1},
                            "claim_count": 0,
                            "output_count": 0,
                            "projection_count": 0,
                            "artifact_count": 0,
                            "event_count": 3,
                        },
                        "documents": [
                            {
                                "document_id": "folder/doc.pdf",
                                "pipeline_id": "pipeline",
                                "steps": [],
                                "statuses": {},
                                "claims": {},
                                "outputs": {},
                                "projections": {},
                                "events": [],
                                "event_count": 3,
                            }
                        ],
                    },
                )
            if request.url.path.endswith("/attach"):
                return httpx.Response(
                    200,
                    json={
                        "pipeline_id": "pipeline",
                        "document_count": 1,
                        "documents": [{"document_id": "folder/doc.pdf"}],
                        "lifecycle": {"ok": True},
                    },
                )
            if request.url.path.endswith("/claim"):
                return httpx.Response(200, json={"process": claim.model_dump(mode="json")})
            if request.url.path.endswith("/schedule"):
                return httpx.Response(200, json={"ok": True, "schedule": {"queued": []}})
            if request.url.path.endswith("/actions"):
                return httpx.Response(
                    200,
                    json={
                        "ok": True,
                        "action": {
                            "pipeline_id": "pipeline",
                            "run_id": "run_http",
                            "document_id": "folder/doc.pdf",
                            "process_id": "extract",
                            "action": "retry",
                            "affected": ["extract"],
                            "schedule": {
                                "pipeline_id": "pipeline",
                                "run_id": "run_http",
                                "document_id": "folder/doc.pdf",
                                "queued": [],
                                "waiting": [],
                                "running": [],
                                "completed": [],
                                "failed": [],
                                "skipped": [],
                                "cancelled": [],
                            },
                        },
                        "lifecycle": {"ok": True},
                    },
                )
            if request.url.path.endswith("/events") and request.method == "GET":
                event = ProcessEvent(
                    id="event_http",
                    run_id="run_http",
                    document_id="folder/doc.pdf",
                    process_id="extract",
                    type="process.progress",
                    status=ProcessStatus.running,
                    data={"percent": 50},
                )
                return httpx.Response(
                    200,
                    json={
                        "ok": True,
                        "run_id": "run_http",
                        "document_id": "folder/doc.pdf",
                        "process_id": "extract",
                        "count": 1,
                        "has_more": False,
                        "next_after_event_id": "event_http",
                        "events": [event.model_dump(mode="json")],
                    },
                )
            return httpx.Response(200, json={"ok": True, "schedule": {}})

        async def run_client() -> ClaimedProcess | None:
            async with ProcessRuntimeClient(
                "http://runtime.test",
                transport=httpx.MockTransport(handler),
            ) as client:
                packages = await client.list_packages()
                self.assertEqual(packages["packages"][0]["id"], "vendor_review")
                package = await client.get_package("vendor_review")
                self.assertEqual(package["package"]["pipeline_ids"], ["pipeline"])
                pipelines = await client.list_pipelines()
                self.assertEqual(pipelines["pipelines"][0]["id"], "pipeline")
                pipeline = await client.get_pipeline("pipeline")
                self.assertEqual(pipeline["package_id"], "vendor_review")
                state = await client.get_state(run_id="run_http")
                self.assertEqual(state.documents[0].event_count, 3)
                self.assertEqual(state.documents[0].events, [])
                attached = await client.attach_run(
                    run_id="run_http",
                    pipeline_id="pipeline",
                )
                self.assertEqual(attached["document_count"], 1)
                claimed = await client.claim_next(
                    run_id="run_http",
                    pipeline_id="pipeline",
                    worker_id="worker-http",
                    adapter_kind="queue",
                )
                await client.write_output(
                    run_id="run_http",
                    document_id="folder/doc.pdf",
                    process_id="extract",
                    output=ProcessOutput(values={"ok": True}),
                    pipeline_id="pipeline",
                    worker_id="worker-http",
                )
                schedule = await client.schedule_document(
                    run_id="run_http",
                    document_id="folder/doc.pdf",
                )
                self.assertEqual(schedule["queued"], [])
                action = await client.control_process(
                    run_id="run_http",
                    document_id="folder/doc.pdf",
                    process_id="extract",
                    action=ProcessAction.retry,
                    pipeline_id="pipeline",
                    reason="operator retry",
                )
                self.assertEqual(action.action, ProcessAction.retry)
                self.assertEqual(action.affected, ["extract"])
                event_page = await client.list_events(
                    run_id="run_http",
                    document_id="folder/doc.pdf",
                    process_id="extract",
                    after_event_id="event_previous",
                    limit=10,
                )
                self.assertEqual(event_page.count, 1)
                self.assertEqual(event_page.events[0].data["percent"], 50)
                return claimed

        claimed = asyncio.run(run_client())

        self.assertIsNotNone(claimed)
        assert claimed is not None
        self.assertEqual(claimed.context.input.values["initial"]["source"], "doc.pdf")
        self.assertEqual(
            requests[0].url.raw_path.decode("utf-8"),
            "/api/process-runtime/packages",
        )
        self.assertEqual(
            requests[1].url.raw_path.decode("utf-8"),
            "/api/process-runtime/packages/vendor_review",
        )
        self.assertEqual(
            requests[2].url.raw_path.decode("utf-8"),
            "/api/process-runtime/pipelines",
        )
        self.assertEqual(
            requests[3].url.raw_path.decode("utf-8"),
            "/api/process-runtime/pipelines/pipeline",
        )
        self.assertEqual(
            requests[4].url.raw_path.decode("utf-8").split("?", 1)[0],
            "/api/runs/run_http/process-runtime",
        )
        self.assertEqual(requests[4].url.params["include_events"], "false")
        self.assertEqual(
            requests[5].url.raw_path.decode("utf-8"),
            "/api/runs/run_http/process-runtime/attach",
        )
        self.assertEqual(json.loads(requests[5].content)["pipeline_id"], "pipeline")
        self.assertEqual(
            requests[6].url.raw_path.decode("utf-8"),
            "/api/runs/run_http/process-runtime/claim",
        )
        self.assertEqual(
            requests[7].url.raw_path.decode("utf-8").split("?", 1)[0],
            "/api/runs/run_http/process-runtime/folder%2Fdoc.pdf/processes/extract/output",
        )
        self.assertEqual(requests[7].url.params["pipeline_id"], "pipeline")
        self.assertEqual(requests[7].url.params["worker_id"], "worker-http")
        self.assertEqual(
            requests[8].url.raw_path.decode("utf-8"),
            "/api/runs/run_http/process-runtime/folder%2Fdoc.pdf/schedule",
        )
        self.assertEqual(
            requests[9].url.raw_path.decode("utf-8"),
            "/api/runs/run_http/process-runtime/folder%2Fdoc.pdf/processes/extract/actions",
        )
        action_payload = json.loads(requests[9].content)
        self.assertEqual(action_payload["action"], "retry")
        self.assertEqual(action_payload["pipeline_id"], "pipeline")
        self.assertEqual(action_payload["reason"], "operator retry")
        self.assertEqual(
            requests[10].url.raw_path.decode("utf-8").split("?", 1)[0],
            "/api/runs/run_http/process-runtime/folder%2Fdoc.pdf/events",
        )
        self.assertEqual(requests[10].url.params["process_id"], "extract")
        self.assertEqual(requests[10].url.params["after_event_id"], "event_previous")
        self.assertEqual(requests[10].url.params["limit"], "10")

    def test_adapter_worker_renews_claim_while_subprocess_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            worker_script = Path(tmp) / "slow_step.py"
            worker_script.write_text(
                textwrap.dedent(
                    """
                    import json
                    import sys
                    import time

                    json.loads(sys.stdin.read())
                    time.sleep(0.03)
                    print(json.dumps({"values": {"ok": True}}))
                    """
                ),
                encoding="utf-8",
            )
            claim = ClaimedProcess(
                pipeline_id="pipeline",
                run_id="run_worker",
                document_id="doc_worker",
                process=ScheduledProcess(
                    id="slow",
                    needs=[],
                    adapter={
                        "kind": "subprocess",
                        "command": [sys.executable, str(worker_script)],
                    },
                    timeout_seconds=30,
                ),
                worker_id="worker-sdk",
                attempt=1,
                claim_expires_at=datetime.now(timezone.utc) + timedelta(seconds=60),
                context=ProcessExecutionContext(
                    pipeline_id="pipeline",
                    run_id="run_worker",
                    document_id="doc_worker",
                    process_id="slow",
                    attempt=1,
                    input=ProcessInput(),
                ),
            )
            client = _FakeRuntimeClient(claim)

            result = asyncio.run(
                AdapterProcessRuntimeWorker(
                    client=client,  # type: ignore[arg-type]
                    pipeline_id="pipeline",
                    worker_id="worker-sdk",
                    adapter_kind="subprocess",
                    lease_seconds=60,
                    renew_interval_seconds=0.01,
                ).run_once(run_id="run_worker")
            )

            self.assertTrue(result.completed)
            self.assertGreaterEqual(len(client.renews), 1)
            self.assertEqual(client.renews[0]["process_id"], "slow")
            self.assertEqual(client.renews[0]["worker_id"], "worker-sdk")
            self.assertEqual(client.outputs[0]["output"].values["ok"], True)

    def test_adapter_process_runtime_worker_runs_claimed_subprocess_via_api(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            worker_script = Path(tmp) / "external_worker.py"
            worker_script.write_text(
                textwrap.dedent(
                    """
                    import json
                    import os
                    import sys

                    ctx = json.loads(sys.stdin.read())
                    print(
                        'PROCESS_RUNTIME_EVENT {"type":"process.progress","status":"running","data":{"stage":"external"}}',
                        file=sys.stderr,
                    )
                    print(json.dumps({
                        "values": {
                            "document": ctx["document_id"],
                            "source": ctx["input"]["values"]["initial"]["source"],
                            "env_worker_process": os.environ["PROCESS_RUNTIME_PROCESS_ID"],
                            "artifact_dir_exists": os.path.isdir(os.environ["PROCESS_RUNTIME_ARTIFACT_DIR"]),
                        }
                    }))
                    """
                ),
                encoding="utf-8",
            )
            claim = ClaimedProcess(
                pipeline_id="pipeline",
                run_id="run_worker",
                document_id="doc_worker",
                process=ScheduledProcess(
                    id="external",
                    needs=[],
                    adapter={
                        "kind": "subprocess",
                        "command": [sys.executable, str(worker_script)],
                        "env": {
                            "PROCESS_RUNTIME_ARTIFACT_ROOT": str(Path(tmp) / "artifacts"),
                        },
                    },
                    timeout_seconds=30,
                ),
                worker_id="adapter-worker",
                attempt=1,
                claim_expires_at=datetime.now(timezone.utc) + timedelta(seconds=60),
                context=ProcessExecutionContext(
                    pipeline_id="pipeline",
                    run_id="run_worker",
                    document_id="doc_worker",
                    process_id="external",
                    attempt=1,
                    input=ProcessInput(values={"initial": {"source": "doc.pdf"}, "needs": {}}),
                ),
            )
            requests: list[httpx.Request] = []

            async def handler(request: httpx.Request) -> httpx.Response:
                requests.append(request)
                if request.url.path.endswith("/claim"):
                    return httpx.Response(200, json={"process": claim.model_dump(mode="json")})
                if request.url.path.endswith("/events"):
                    event_payload = json.loads(request.content)
                    return httpx.Response(
                        200,
                        json={
                            "ok": True,
                            "event": event_payload,
                        },
                    )
                if request.url.path.endswith("/output"):
                    return httpx.Response(200, json={"ok": True, "schedule": {}})
                return httpx.Response(500, json={"error": f"unexpected {request.url}"})

            async def run_worker() -> ProcessWorkerResult:
                async with ProcessRuntimeClient(
                    "http://runtime.test",
                    transport=httpx.MockTransport(handler),
                ) as client:
                    return await AdapterProcessRuntimeWorker(
                        client=client,
                        pipeline_id="pipeline",
                        worker_id="adapter-worker",
                        adapter_kind="subprocess",
                    ).run_once(run_id="run_worker")

            result = asyncio.run(run_worker())

            self.assertTrue(result.completed)
            self.assertIsNone(result.error)
            self.assertEqual(requests[0].url.path, "/api/runs/run_worker/process-runtime/claim")
            event_requests = [request for request in requests if request.url.path.endswith("/events")]
            self.assertEqual(
                [json.loads(request.content)["type"] for request in event_requests],
                ["process.started", "process.progress"],
            )
            progress_payload = json.loads(event_requests[1].content)
            self.assertEqual(progress_payload["status"], "running")
            self.assertEqual(progress_payload["data"]["stage"], "external")
            output_request = next(request for request in requests if request.url.path.endswith("/output"))
            output_payload = json.loads(output_request.content)
            self.assertEqual(output_payload["values"]["document"], "doc_worker")
            self.assertEqual(output_payload["values"]["source"], "doc.pdf")
            self.assertEqual(output_payload["values"]["env_worker_process"], "external")
            self.assertTrue(output_payload["values"]["artifact_dir_exists"])
            self.assertEqual(output_payload["metadata"]["process_runtime"]["adapter_kind"], "subprocess")
            self.assertEqual(output_payload["metadata"]["process_runtime"]["exit_code"], 0)
            self.assertEqual(output_payload["metadata"]["process_runtime"]["event_count"], 1)
            self.assertEqual(output_request.url.params["pipeline_id"], "pipeline")
            self.assertEqual(output_request.url.params["worker_id"], "adapter-worker")

    def test_adapter_process_runtime_worker_runs_queue_claim_through_external_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            worker_script = Path(tmp) / "queue_step.py"
            worker_script.write_text(
                textwrap.dedent(
                    """
                    import json
                    import os
                    import sys

                    ctx = json.loads(sys.stdin.read())
                    print(
                        'PROCESS_RUNTIME_EVENT {"type":"process.progress","status":"running","data":{"stage":"queue-command"}}',
                        file=sys.stderr,
                    )
                    print(json.dumps({
                        "values": {
                            "document": ctx["document_id"],
                            "process": ctx["process_id"],
                            "source": ctx["input"]["values"]["initial"]["source"],
                            "artifact_dir_exists": os.path.isdir(os.environ["PROCESS_RUNTIME_ARTIFACT_DIR"]),
                        }
                    }))
                    """
                ),
                encoding="utf-8",
            )
            claim = ClaimedProcess(
                pipeline_id="pipeline",
                run_id="run_worker",
                document_id="doc_worker",
                process=ScheduledProcess(
                    id="external_queue",
                    needs=[],
                    adapter={"kind": "queue", "queue": "demo.external"},
                    timeout_seconds=30,
                ),
                worker_id="queue-command-worker",
                attempt=1,
                claim_expires_at=datetime.now(timezone.utc) + timedelta(seconds=60),
                context=ProcessExecutionContext(
                    pipeline_id="pipeline",
                    run_id="run_worker",
                    document_id="doc_worker",
                    process_id="external_queue",
                    attempt=1,
                    input=ProcessInput(values={"initial": {"source": "doc.pdf"}, "needs": {}}),
                ),
            )
            requests: list[httpx.Request] = []

            async def handler(request: httpx.Request) -> httpx.Response:
                requests.append(request)
                if request.url.path.endswith("/claim"):
                    return httpx.Response(200, json={"process": claim.model_dump(mode="json")})
                if request.url.path.endswith("/events"):
                    return httpx.Response(
                        200,
                        json={"ok": True, "event": json.loads(request.content)},
                    )
                if request.url.path.endswith("/output"):
                    return httpx.Response(200, json={"ok": True, "schedule": {}})
                return httpx.Response(500, json={"error": f"unexpected {request.url}"})

            async def run_worker() -> ProcessWorkerResult:
                async with ProcessRuntimeClient(
                    "http://runtime.test",
                    transport=httpx.MockTransport(handler),
                ) as client:
                    return await AdapterProcessRuntimeWorker(
                        client=client,
                        pipeline_id="pipeline",
                        worker_id="queue-command-worker",
                        adapter_kind="queue",
                        adapters=AdapterRegistry(
                            {
                                "queue": ExternalCommandAdapter(
                                    command=[sys.executable, str(worker_script)],
                                    env={
                                        "PROCESS_RUNTIME_ARTIFACT_ROOT": str(Path(tmp) / "artifacts"),
                                    },
                                )
                            }
                        ),
                    ).run_once(run_id="run_worker")

            result = asyncio.run(run_worker())

            self.assertTrue(result.completed)
            self.assertIsNone(result.error)
            claim_payload = json.loads(requests[0].content)
            self.assertEqual(claim_payload["adapter_kind"], "queue")
            event_requests = [request for request in requests if request.url.path.endswith("/events")]
            self.assertEqual(
                [json.loads(request.content)["type"] for request in event_requests],
                ["process.started", "process.progress"],
            )
            output_request = next(request for request in requests if request.url.path.endswith("/output"))
            output_payload = json.loads(output_request.content)
            self.assertEqual(output_payload["values"]["document"], "doc_worker")
            self.assertEqual(output_payload["values"]["process"], "external_queue")
            self.assertTrue(output_payload["values"]["artifact_dir_exists"])
            runtime_metadata = output_payload["metadata"]["process_runtime"]
            self.assertEqual(runtime_metadata["adapter_kind"], "subprocess")
            self.assertEqual(runtime_metadata["execution_adapter_kind"], "external_command")
            self.assertEqual(runtime_metadata["claimed_adapter_kind"], "queue")

    def test_process_runtime_worker_cli_requires_command_for_queue_claims(self) -> None:
        buffer = StringIO()
        with redirect_stdout(buffer):
            code = runtime_worker_cli_main(
                [
                    "--base-url",
                    "http://runtime.test",
                    "--run-id",
                    "run_worker",
                    "--pipeline",
                    "pipeline",
                    "--worker-id",
                    "queue-worker",
                    "--adapter-kind",
                    "queue",
                ]
            )

        self.assertEqual(code, 1)
        payload = json.loads(buffer.getvalue())
        self.assertFalse(payload["ok"])
        self.assertIn("requires --command", payload["error"])

    def test_process_runtime_worker_cli_command_can_contain_child_flags(self) -> None:
        args = build_runtime_worker_parser().parse_args(
            [
                "--base-url",
                "http://runtime.test",
                "--run-id",
                "run_worker",
                "--pipeline",
                "pipeline",
                "--worker-id",
                "queue-worker",
                "--adapter-kind",
                "queue",
                "--command",
                "uv",
                "run",
                "--project",
                "vendor-worker",
                "vendor-step-enrich",
                "--mode",
                "strict",
            ]
        )

        self.assertEqual(
            args.command,
            [
                "uv",
                "run",
                "--project",
                "vendor-worker",
                "vendor-step-enrich",
                "--mode",
                "strict",
            ],
        )

    def test_process_runtime_worker_cli_runs_package_worker_manifest_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "demo_package"
            package_dir.mkdir()
            step_script = package_dir / "steps" / "enrich.py"
            step_script.parent.mkdir()
            step_script.write_text(
                textwrap.dedent(
                    """
                    import json
                    import sys

                    ctx = json.loads(sys.stdin.read())
                    print(json.dumps({
                        "values": {
                            "document": ctx["document_id"],
                            "process": ctx["process_id"],
                        }
                    }))
                    """
                ),
                encoding="utf-8",
            )
            (package_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: demo_package
                    pipelines:
                      - demo.yaml
                    workers:
                      - id: enrich_worker
                        pipeline: demo_flow
                        process: enrich
                        command: ["python", "steps/enrich.py"]
                        cwd: "."
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "demo.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: demo_flow
                    steps:
                      - id: enrich
                        adapter:
                          kind: queue
                          queue: demo.enrich
                    """
                ).strip(),
                encoding="utf-8",
            )
            claim = ClaimedProcess(
                pipeline_id="demo_flow",
                run_id="run_worker",
                document_id="doc_worker",
                process=ScheduledProcess(
                    id="enrich",
                    needs=[],
                    adapter={"kind": "queue", "queue": "demo.enrich"},
                ),
                worker_id="enrich_worker",
                attempt=1,
                claim_expires_at=datetime.now(timezone.utc) + timedelta(seconds=60),
                context=ProcessExecutionContext(
                    pipeline_id="demo_flow",
                    run_id="run_worker",
                    document_id="doc_worker",
                    process_id="enrich",
                    attempt=1,
                    input=ProcessInput(values={"initial": {}, "needs": {}}),
                ),
            )

            class FakeClient(_FakeRuntimeClient):
                def __init__(self, *_args, **_kwargs) -> None:
                    super().__init__(claim)

                async def __aenter__(self):
                    return self

                async def __aexit__(self, *_args):
                    return None

            buffer = StringIO()
            with patch(
                "fala.worker_cli.ProcessRuntimeClient",
                FakeClient,
            ), redirect_stdout(buffer):
                code = runtime_worker_cli_main(
                    [
                        "--pipeline-dir",
                        str(root),
                        "--base-url",
                        "http://runtime.test",
                        "--run-id",
                        "run_worker",
                        "--package-worker",
                        "enrich_worker",
                    ]
                )

            self.assertEqual(code, 0, buffer.getvalue())
            payload = json.loads(buffer.getvalue())
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["pipeline_id"], "demo_flow")
            self.assertEqual(payload["process_id"], "enrich")
            self.assertEqual(payload["worker_id"], "enrich_worker")
            self.assertEqual(payload["adapter_kind"], "queue")
            self.assertEqual(payload["completed_count"], 1)

    def test_runner_executes_processes_and_writes_combine_latest_projection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            extract_script = root / "extract_text.py"
            extract_script.write_text(
                textwrap.dedent(
                    """
                    import json
                    import sys

                    ctx = json.loads(sys.stdin.read())
                    text = ctx["input"]["values"]["initial"]["text"]
                    print(json.dumps({"values": {"text": text}}))
                    """
                ),
                encoding="utf-8",
            )
            parse_script = root / "parse_metadata.py"
            parse_script.write_text(
                textwrap.dedent(
                    """
                    import json
                    import sys

                    ctx = json.loads(sys.stdin.read())
                    text = ctx["input"]["values"]["needs"]["extract_text"]["text"]
                    print(json.dumps({"values": {"title": text.splitlines()[0]}}))
                    """
                ),
                encoding="utf-8",
            )
            classify_script = root / "classify_hazards.py"
            classify_script.write_text(
                textwrap.dedent(
                    """
                    import json
                    import sys

                    ctx = json.loads(sys.stdin.read())
                    text = ctx["input"]["values"]["needs"]["extract_text"]["text"]
                    hazards = ["H315"] if "H315" in text else []
                    print(json.dumps({"values": {"hazards": hazards}}))
                    """
                ),
                encoding="utf-8",
            )
            pipeline = PipelineSpec(
                id="document_test",
                steps=[
                    ProcessSpec(
                        id="extract_text",
                        adapter=AdapterSpec(
                            kind="subprocess",
                            command=[sys.executable, str(extract_script)],
                        ),
                    ),
                    ProcessSpec(
                        id="parse_metadata",
                        needs=["extract_text"],
                        adapter=AdapterSpec(
                            kind="subprocess",
                            command=[sys.executable, str(parse_script)],
                        ),
                    ),
                    ProcessSpec(
                        id="classify_hazards",
                        needs=["extract_text"],
                        adapter=AdapterSpec(
                            kind="subprocess",
                            command=[sys.executable, str(classify_script)],
                        ),
                    ),
                ],
                combines=[
                    CombineSpec(
                        id="document_enrichment",
                        needs=["parse_metadata", "classify_hazards"],
                    )
                ],
            )

            result = asyncio.run(
                PipelineRunner(
                    pipeline,
                    store=InMemoryStateStore(),
                ).run_document(
                    run_id="run_1",
                    document_id="doc_1",
                    values={"text": "Document sample\nContains H315"},
                )
            )

        self.assertEqual(result.outputs["extract_text"].values["text"], "Document sample\nContains H315")
        projection = result.projections["document_enrichment"]
        self.assertTrue(projection.complete)
        self.assertEqual(projection.latest["parse_metadata"].values["title"], "Document sample")
        self.assertEqual(projection.latest["classify_hazards"].values["hazards"], ["H315"])
        self.assertIn("projection.updated", [event.type for event in result.events])

    def test_yaml_loader_accepts_pipeline_alias_and_validates_dependencies(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            valid = Path(tmp) / "pipeline.yaml"
            valid.write_text(
                textwrap.dedent(
                    """
                    pipeline: demo
                    title: Demo pipeline
                    description: Demo workflow
                    tags: [demo, generic]
                    steps:
                      - id: first
                        title: First step
                        description: First worker boundary
                        tags: [root]
                        adapter:
                          kind: queue
                          queue: tests.first
                    combines:
                      - id: bundle
                        needs: [first]
                    """
                ),
                encoding="utf-8",
            )
            spec = load_pipeline_yaml(valid)
            self.assertEqual(spec.id, "demo")
            self.assertEqual(spec.title, "Demo pipeline")
            self.assertEqual(spec.description, "Demo workflow")
            self.assertEqual(spec.tags, ["demo", "generic"])
            self.assertEqual(spec.combines[0].id, "bundle")
            self.assertEqual(spec.steps[0].title, "First step")
            self.assertEqual(spec.steps[0].description, "First worker boundary")
            self.assertEqual(spec.steps[0].tags, ["root"])
            self.assertIsNone(spec.steps[0].adapter.cwd)

            invalid = Path(tmp) / "invalid.yaml"
            invalid.write_text(
                textwrap.dedent(
                    """
                    pipeline: bad
                    steps:
                      - id: second
                        needs: [missing]
                        adapter:
                          kind: queue
                          queue: tests.second
                    """
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ValidationError):
                load_pipeline_yaml(invalid)

    def test_yaml_loader_resolves_relative_subprocess_cwd_from_pipeline_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipeline_dir = root / "config" / "pipelines"
            pipeline_dir.mkdir(parents=True)
            pipeline_path = pipeline_dir / "pipeline.yaml"
            pipeline_path.write_text(
                textwrap.dedent(
                    """
                    pipeline: demo
                    steps:
                      - id: first
                        adapter:
                          kind: subprocess
                          command: ["python", "-c", "print('{}')"]
                          cwd: "../.."
                    """
                ),
                encoding="utf-8",
            )

            spec = load_pipeline_yaml(pipeline_path)

            self.assertEqual(spec.steps[0].adapter.cwd, str(root.resolve()))

    def test_pipeline_registry_loads_yaml_configs_and_rejects_duplicate_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "one.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: one
                    steps:
                      - id: first
                        adapter:
                          kind: queue
                          queue: tests.first
                    """
                ),
                encoding="utf-8",
            )
            (root / "two.yml").write_text(
                textwrap.dedent(
                    """
                    pipeline: two
                    steps:
                      - id: first
                        adapter:
                          kind: queue
                          queue: tests.first
                    """
                ),
                encoding="utf-8",
            )
            registry = PipelineRegistry.from_directory(root)
            self.assertEqual([spec.id for spec in registry.all()], ["one", "two"])
            self.assertEqual(registry.get("one").id, "one")

            (root / "duplicate.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: one
                    steps:
                      - id: other
                        adapter:
                          kind: queue
                          queue: tests.other
                    """
                ),
                encoding="utf-8",
            )
            with self.assertRaises(PipelineRegistryError):
                PipelineRegistry.from_directory(root)

    def test_subprocess_adapter_runs_process_in_isolation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            script = Path(tmp) / "worker.py"
            script.write_text(
                textwrap.dedent(
                    """
                    import json
                    import os
                    import sys

                    print("native warning on stderr", file=sys.stderr)
                    print(
                        'PROCESS_RUNTIME_EVENT {"type":"process.progress","status":"running","data":{"percent":50}}',
                        file=sys.stderr,
                    )
                    ctx = json.loads(sys.stdin.read())
                    initial = ctx["input"]["values"]["initial"]
                    print(json.dumps({
                        "values": {
                            "worker_pid_visible": True,
                            "document": ctx["document_id"],
                            "source": initial["source"],
                            "env_run_id": os.environ["PROCESS_RUNTIME_RUN_ID"],
                            "env_document_id": os.environ["PROCESS_RUNTIME_DOCUMENT_ID"],
                            "env_process_id": os.environ["PROCESS_RUNTIME_PROCESS_ID"],
                            "env_attempt": os.environ["PROCESS_RUNTIME_ATTEMPT"],
                            "artifact_dir_exists": os.path.isdir(os.environ["PROCESS_RUNTIME_ARTIFACT_DIR"]),
                        }
                    }))
                    """
                ),
                encoding="utf-8",
            )
            pipeline = PipelineSpec(
                id="subprocess_test",
                steps=[
                    ProcessSpec(
                        id="external_enrichment",
                        adapter=AdapterSpec(
                            kind="subprocess",
                            command=[sys.executable, str(script)],
                            env={
                                "PROCESS_RUNTIME_ARTIFACT_ROOT": str(Path(tmp) / "artifacts"),
                            },
                        ),
                    )
                ],
                combines=[CombineSpec(id="bundle", needs=["external_enrichment"])],
            )

            result = asyncio.run(
                PipelineRunner(pipeline).run_document(
                    run_id="run_sub",
                    document_id="doc_sub",
                    values={"source": "document.pdf"},
                )
            )

        output = result.outputs["external_enrichment"].values
        self.assertEqual(output["document"], "doc_sub")
        self.assertEqual(output["source"], "document.pdf")
        self.assertEqual(output["env_run_id"], "run_sub")
        self.assertEqual(output["env_document_id"], "doc_sub")
        self.assertEqual(output["env_process_id"], "external_enrichment")
        self.assertEqual(output["env_attempt"], "1")
        self.assertTrue(output["artifact_dir_exists"])
        metadata = result.outputs["external_enrichment"].metadata["process_runtime"]
        self.assertEqual(metadata["adapter_kind"], "subprocess")
        self.assertEqual(metadata["exit_code"], 0)
        self.assertEqual(metadata["event_count"], 1)
        self.assertGreaterEqual(metadata["duration_seconds"], 0)
        self.assertIn("native warning on stderr", metadata["stderr_tail"])
        event = next(item for item in result.events if item.type == "process.progress")
        self.assertEqual(event.status, ProcessStatus.running)
        self.assertEqual(event.data["percent"], 50)
        self.assertTrue(result.projections["bundle"].complete)

    def test_subprocess_adapter_accepts_json_line_with_trailing_stdout_noise(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            script = Path(tmp) / "noisy_worker.py"
            script.write_text(
                textwrap.dedent(
                    """
                    import json

                    print(json.dumps({"values": {"ok": True}}))
                    print("native framework warning")
                    """
                ),
                encoding="utf-8",
            )
            pipeline = PipelineSpec(
                id="subprocess_noise_test",
                steps=[
                    ProcessSpec(
                        id="noisy",
                        adapter=AdapterSpec(
                            kind="subprocess",
                            command=[sys.executable, str(script)],
                            env={
                                "PROCESS_RUNTIME_ARTIFACT_ROOT": str(Path(tmp) / "artifacts"),
                            },
                        ),
                    )
                ],
            )

            result = asyncio.run(
                PipelineRunner(pipeline).run_document(
                    run_id="run_noise",
                    document_id="doc_noise",
                )
            )

        self.assertTrue(result.outputs["noisy"].values["ok"])

    def test_queue_adapter_requires_external_worker_execution(self) -> None:
        pipeline = PipelineSpec(
            id="queue_test",
            steps=[
                ProcessSpec(
                    id="queued_enrichment",
                    adapter=AdapterSpec(kind="queue", queue="workflow.enrich"),
                    config={"queue_name": "workflow.enrich"},
                )
            ],
            combines=[CombineSpec(id="bundle", needs=["queued_enrichment"])],
        )

        with self.assertRaises(PipelineRunError) as failure:
            asyncio.run(
                PipelineRunner(pipeline).run_document(
                    run_id="run_queue",
                    document_id="doc_queue",
                    values={},
                )
            )
        self.assertIn("cannot run in-process", str(failure.exception))

    def test_scheduler_initializes_waiting_and_queues_ready_steps(self) -> None:
        store = InMemoryStateStore()
        pipeline = PipelineSpec(
            id="scheduler_test",
            steps=[
                ProcessSpec(
                    id="first",
                    adapter=AdapterSpec(kind="queue", queue="test.first"),
                ),
                ProcessSpec(
                    id="second",
                    needs=["first"],
                    adapter=AdapterSpec(kind="queue", queue="test.second"),
                ),
            ],
            combines=[CombineSpec(id="bundle", needs=["second"])],
        )
        scheduler = PipelineScheduler(pipeline, store)

        initialized = asyncio.run(
            scheduler.initialize_document(
                run_id="run_schedule",
                document_id="doc_schedule",
                values={"source": "sample.pdf"},
                artifacts=[
                    ArtifactRef(
                        kind="pdf",
                        uri="file:///tmp/sample.pdf",
                    )
                ],
            )
        )

        self.assertEqual([process.id for process in initialized.queued], ["first"])
        self.assertEqual(initialized.waiting, ["second"])
        statuses = asyncio.run(
            store.list_statuses(run_id="run_schedule", document_id="doc_schedule")
        )
        self.assertEqual(statuses["first"].value, "queued")
        self.assertEqual(statuses["second"].value, "waiting")
        stored_pipeline_id = asyncio.run(
            store.get_document_pipeline_id(
                run_id="run_schedule",
                document_id="doc_schedule",
            )
        )
        self.assertEqual(stored_pipeline_id, "scheduler_test")
        asyncio.run(
            scheduler.initialize_document(
                run_id="run_schedule",
                document_id="doc_schedule",
                values={"source": "wrong.pdf"},
            )
        )
        other_pipeline = PipelineSpec(
            id="other_pipeline",
            steps=[
                ProcessSpec(
                    id="first",
                    adapter=AdapterSpec(kind="queue", queue="test.first"),
                )
            ],
        )
        with self.assertRaises(ValueError):
            asyncio.run(
                PipelineScheduler(other_pipeline, store).initialize_document(
                    run_id="run_schedule",
                    document_id="doc_schedule",
                    values={},
                )
            )

        asyncio.run(
            store.put_output(
                run_id="run_schedule",
                document_id="doc_schedule",
                process_id="first",
                output=ProcessOutput(values={"ok": True}),
            )
        )
        scheduled = asyncio.run(
            scheduler.schedule_ready(run_id="run_schedule", document_id="doc_schedule")
        )

        self.assertEqual(scheduled.completed, ["first"])
        self.assertEqual([process.id for process in scheduled.queued], ["second"])
        statuses = asyncio.run(
            store.list_statuses(run_id="run_schedule", document_id="doc_schedule")
        )
        self.assertEqual(statuses["first"].value, "completed")
        self.assertEqual(statuses["second"].value, "queued")

        claim = asyncio.run(
            scheduler.claim_next(
                run_id="run_schedule",
                document_ids=["doc_schedule"],
                worker_id="worker-ctx",
                adapter_kind="queue",
            )
        )

        self.assertIsNotNone(claim)
        assert claim is not None
        self.assertEqual(claim.process.id, "second")
        self.assertEqual(claim.context.process_id, "second")
        self.assertEqual(claim.context.input.values["initial"], {"source": "sample.pdf"})
        self.assertEqual(claim.context.input.values["needs"]["first"], {"ok": True})
        self.assertEqual(claim.context.input.artifacts[0].uri, "file:///tmp/sample.pdf")

    def test_scheduler_claims_next_queued_process(self) -> None:
        store = InMemoryStateStore()
        pipeline = PipelineSpec(
            id="claim_test",
            steps=[
                ProcessSpec(
                    id="extract",
                    adapter=AdapterSpec(kind="queue", queue="test.extract"),
                )
            ],
        )
        scheduler = PipelineScheduler(pipeline, store)
        asyncio.run(
            scheduler.initialize_document(
                run_id="run_claim",
                document_id="doc_claim",
                values={},
            )
        )

        claim = asyncio.run(
            scheduler.claim_next(
                run_id="run_claim",
                document_ids=["doc_claim"],
                worker_id="worker-1",
                adapter_kind="queue",
            )
        )

        self.assertIsNotNone(claim)
        assert claim is not None
        self.assertEqual(claim.document_id, "doc_claim")
        self.assertEqual(claim.process.id, "extract")
        self.assertEqual(claim.worker_id, "worker-1")
        self.assertEqual(claim.attempt, 1)
        statuses = asyncio.run(
            store.list_statuses(run_id="run_claim", document_id="doc_claim")
        )
        self.assertEqual(statuses["extract"].value, "running")

        renewed = asyncio.run(
            scheduler.renew_claim(
                run_id="run_claim",
                document_id="doc_claim",
                process_id="extract",
                worker_id="worker-1",
                lease_seconds=120,
            )
        )
        self.assertIsNotNone(renewed)
        assert renewed is not None
        self.assertEqual(renewed.attempt, 1)
        self.assertEqual(renewed.worker_id, "worker-1")

        wrong_worker_renewal = asyncio.run(
            scheduler.renew_claim(
                run_id="run_claim",
                document_id="doc_claim",
                process_id="extract",
                worker_id="worker-2",
                lease_seconds=120,
            )
        )
        self.assertIsNone(wrong_worker_renewal)

        second_claim = asyncio.run(
            scheduler.claim_next(
                run_id="run_claim",
                document_ids=["doc_claim"],
                worker_id="worker-2",
            )
        )
        self.assertIsNone(second_claim)

    def test_sqlite_scheduler_claims_process_atomically_across_store_instances(self) -> None:
        pipeline = PipelineSpec(
            id="sqlite_claim_race",
            steps=[
                ProcessSpec(
                    id="extract",
                    adapter=AdapterSpec(kind="queue", queue="test.extract"),
                )
            ],
        )

        async def race(db_path: Path) -> tuple[list[object | None], dict[str, ProcessStatus], dict[str, object]]:
            setup_store = SQLiteStateStore(db_path)
            await PipelineScheduler(pipeline, setup_store).initialize_document(
                run_id="run_race",
                document_id="doc_race",
                values={},
            )

            scheduler_1 = PipelineScheduler(pipeline, SQLiteStateStore(db_path))
            scheduler_2 = PipelineScheduler(pipeline, SQLiteStateStore(db_path))
            claims = await asyncio.gather(
                scheduler_1.claim_next(
                    run_id="run_race",
                    document_ids=["doc_race"],
                    worker_id="worker-1",
                    lease_seconds=120,
                ),
                scheduler_2.claim_next(
                    run_id="run_race",
                    document_ids=["doc_race"],
                    worker_id="worker-2",
                    lease_seconds=120,
                ),
            )
            final_store = SQLiteStateStore(db_path)
            statuses = await final_store.list_statuses(
                run_id="run_race",
                document_id="doc_race",
            )
            claims_by_process = await final_store.list_claims(
                run_id="run_race",
                document_id="doc_race",
            )
            return claims, statuses, claims_by_process

        with tempfile.TemporaryDirectory() as tmp:
            claims, statuses, claims_by_process = asyncio.run(
                race(Path(tmp) / "runtime.db")
            )

        claimed = [claim for claim in claims if claim is not None]
        self.assertEqual(len(claimed), 1)
        self.assertEqual(statuses["extract"], ProcessStatus.running)
        self.assertEqual(set(claims_by_process), {"extract"})
        self.assertIn(claims_by_process["extract"].worker_id, {"worker-1", "worker-2"})

    def test_state_store_lists_events_with_process_cursor_and_limit(self) -> None:
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)

        def events() -> list[ProcessEvent]:
            return [
                ProcessEvent(
                    id="event_a",
                    run_id="run_events",
                    document_id="doc_events",
                    process_id="extract",
                    type="process.started",
                    ts=base,
                ),
                ProcessEvent(
                    id="event_b",
                    run_id="run_events",
                    document_id="doc_events",
                    process_id="classify",
                    type="process.started",
                    ts=base + timedelta(seconds=1),
                ),
                ProcessEvent(
                    id="event_c",
                    run_id="run_events",
                    document_id="doc_events",
                    process_id="extract",
                    type="process.progress",
                    ts=base + timedelta(seconds=2),
                ),
                ProcessEvent(
                    id="event_d",
                    run_id="run_events",
                    document_id="doc_events",
                    process_id="extract",
                    type="process.completed",
                    ts=base + timedelta(seconds=3),
                ),
            ]

        async def exercise(store) -> None:
            for event in events():
                await store.append_event(event)

            first_page = await store.list_events(
                run_id="run_events",
                document_id="doc_events",
                process_id="extract",
                limit=2,
            )
            self.assertEqual([event.id for event in first_page], ["event_a", "event_c"])

            second_page = await store.list_events(
                run_id="run_events",
                document_id="doc_events",
                process_id="extract",
                after_event_id="event_c",
                limit=2,
            )
            self.assertEqual([event.id for event in second_page], ["event_d"])

            latest = await store.list_events(
                run_id="run_events",
                document_id="doc_events",
                process_id="extract",
                limit=2,
                descending=True,
            )
            self.assertEqual([event.id for event in latest], ["event_d", "event_c"])
            self.assertEqual(
                await store.count_events(
                    run_id="run_events",
                    document_id="doc_events",
                ),
                4,
            )
            self.assertEqual(
                await store.count_events(
                    run_id="run_events",
                    document_id="doc_events",
                    process_id="extract",
                ),
                3,
            )

            with self.assertRaises(ValueError):
                await store.list_events(
                    run_id="run_events",
                    document_id="doc_events",
                    process_id="extract",
                    after_event_id="event_b",
                )

        asyncio.run(exercise(InMemoryStateStore()))
        with tempfile.TemporaryDirectory() as tmp:
            asyncio.run(exercise(SQLiteStateStore(Path(tmp) / "runtime.sqlite")))

    def test_scheduler_retries_expired_claims_and_fails_after_max_attempts(self) -> None:
        store = InMemoryStateStore()
        pipeline = PipelineSpec(
            id="retry_claim_test",
            steps=[
                ProcessSpec(
                    id="extract",
                    adapter=AdapterSpec(kind="queue", queue="test.extract"),
                    retry=RetryPolicy(max_attempts=2),
                )
            ],
        )
        scheduler = PipelineScheduler(pipeline, store)
        asyncio.run(
            scheduler.initialize_document(
                run_id="run_retry_claim",
                document_id="doc_retry_claim",
                values={},
            )
        )

        first = asyncio.run(
            scheduler.claim_next(
                run_id="run_retry_claim",
                document_ids=["doc_retry_claim"],
                worker_id="worker-1",
                lease_seconds=0,
            )
        )
        self.assertIsNotNone(first)
        assert first is not None
        self.assertEqual(first.attempt, 1)

        second = asyncio.run(
            scheduler.claim_next(
                run_id="run_retry_claim",
                document_ids=["doc_retry_claim"],
                worker_id="worker-2",
                lease_seconds=0,
            )
        )
        self.assertIsNotNone(second)
        assert second is not None
        self.assertEqual(second.attempt, 2)
        self.assertEqual(second.worker_id, "worker-2")

        third = asyncio.run(
            scheduler.claim_next(
                run_id="run_retry_claim",
                document_ids=["doc_retry_claim"],
                worker_id="worker-3",
                lease_seconds=0,
            )
        )
        self.assertIsNone(third)
        statuses = asyncio.run(
            store.list_statuses(run_id="run_retry_claim", document_id="doc_retry_claim")
        )
        self.assertEqual(statuses["extract"].value, "failed")
        events = asyncio.run(
            store.list_events(run_id="run_retry_claim", document_id="doc_retry_claim")
        )
        self.assertIn("process.claim_expired", [event.type for event in events])

    def test_scheduler_retries_process_failure_until_max_attempts(self) -> None:
        store = InMemoryStateStore()
        pipeline = PipelineSpec(
            id="retry_failure_test",
            steps=[
                ProcessSpec(
                    id="extract",
                    adapter=AdapterSpec(kind="queue", queue="test.extract"),
                    retry=RetryPolicy(max_attempts=2),
                )
            ],
        )
        scheduler = PipelineScheduler(pipeline, store)
        asyncio.run(
            scheduler.initialize_document(
                run_id="run_retry_failure",
                document_id="doc_retry_failure",
                values={},
            )
        )

        first = asyncio.run(
            scheduler.claim_next(
                run_id="run_retry_failure",
                document_ids=["doc_retry_failure"],
                worker_id="worker-1",
            )
        )
        self.assertIsNotNone(first)
        assert first is not None
        self.assertEqual(first.attempt, 1)

        first_failure = asyncio.run(
            scheduler.record_process_failure(
                run_id="run_retry_failure",
                document_id="doc_retry_failure",
                process_id="extract",
                data={"error": "boom"},
            )
        )
        self.assertEqual(first_failure.action, "retry")
        self.assertEqual([process.id for process in first_failure.schedule.queued], ["extract"])
        statuses = asyncio.run(
            store.list_statuses(run_id="run_retry_failure", document_id="doc_retry_failure")
        )
        self.assertEqual(statuses["extract"], ProcessStatus.queued)

        second = asyncio.run(
            scheduler.claim_next(
                run_id="run_retry_failure",
                document_ids=["doc_retry_failure"],
                worker_id="worker-2",
            )
        )
        self.assertIsNotNone(second)
        assert second is not None
        self.assertEqual(second.attempt, 2)

        second_failure = asyncio.run(
            scheduler.record_process_failure(
                run_id="run_retry_failure",
                document_id="doc_retry_failure",
                process_id="extract",
                data={"error": "boom again"},
            )
        )
        self.assertEqual(second_failure.action, "fail")
        statuses = asyncio.run(
            store.list_statuses(run_id="run_retry_failure", document_id="doc_retry_failure")
        )
        self.assertEqual(statuses["extract"], ProcessStatus.failed)
        claim = asyncio.run(
            store.get_claim(
                run_id="run_retry_failure",
                document_id="doc_retry_failure",
                process_id="extract",
            )
        )
        self.assertIsNone(claim)
        events = asyncio.run(
            store.list_events(run_id="run_retry_failure", document_id="doc_retry_failure")
        )
        self.assertIn("process.retry_scheduled", [event.type for event in events])
        self.assertIn("process.failed", [event.type for event in events])

    def test_process_control_retry_skip_fail_and_cancel_manage_downstream_state(self) -> None:
        store = InMemoryStateStore()
        pipeline = PipelineSpec(
            id="control_test",
            steps=[
                ProcessSpec(
                    id="first",
                    adapter=AdapterSpec(kind="queue", queue="test.first"),
                ),
                ProcessSpec(
                    id="second",
                    needs=["first"],
                    adapter=AdapterSpec(kind="queue", queue="test.second"),
                ),
            ],
            combines=[CombineSpec(id="bundle", needs=["first", "second"])],
        )
        scheduler = PipelineScheduler(pipeline, store)
        asyncio.run(
            scheduler.initialize_document(
                run_id="run_control",
                document_id="doc_control",
                values={},
            )
        )
        asyncio.run(
            store.put_output(
                run_id="run_control",
                document_id="doc_control",
                process_id="first",
                output=ProcessOutput(values={"value": "old-first"}),
            )
        )
        asyncio.run(
            store.put_output(
                run_id="run_control",
                document_id="doc_control",
                process_id="second",
                output=ProcessOutput(values={"value": "old-second"}),
            )
        )
        asyncio.run(
            store.set_status(
                run_id="run_control",
                document_id="doc_control",
                process_id="first",
                status=ProcessStatus.completed,
            )
        )
        asyncio.run(
            store.set_status(
                run_id="run_control",
                document_id="doc_control",
                process_id="second",
                status=ProcessStatus.completed,
            )
        )

        retry = asyncio.run(
            scheduler.retry_process(
                run_id="run_control",
                document_id="doc_control",
                process_id="first",
                reason="operator retry",
            )
        )

        self.assertEqual(retry.affected, ["first", "second"])
        self.assertEqual([process.id for process in retry.schedule.queued], ["first"])
        self.assertEqual(retry.schedule.waiting, ["second"])
        outputs = asyncio.run(
            store.list_outputs(run_id="run_control", document_id="doc_control")
        )
        self.assertEqual(outputs, {})

        skip = asyncio.run(
            scheduler.skip_process(
                run_id="run_control",
                document_id="doc_control",
                process_id="first",
                reason="operator skip",
            )
        )
        self.assertEqual(skip.action, "skip")
        self.assertEqual(skip.schedule.skipped, ["first"])
        self.assertEqual([process.id for process in skip.schedule.queued], ["second"])
        first_output = asyncio.run(
            store.get_output(
                run_id="run_control",
                document_id="doc_control",
                process_id="first",
            )
        )
        self.assertIsNotNone(first_output)
        assert first_output is not None
        self.assertEqual(first_output.values["status"], "skipped")

        failed = asyncio.run(
            scheduler.fail_process(
                run_id="run_control",
                document_id="doc_control",
                process_id="first",
                reason="operator fail",
            )
        )
        self.assertEqual(failed.action, "fail")
        statuses = asyncio.run(
            store.list_statuses(run_id="run_control", document_id="doc_control")
        )
        self.assertEqual(statuses["first"].value, "failed")
        self.assertEqual(statuses["second"].value, "waiting")

        cancelled = asyncio.run(
            scheduler.cancel_process(
                run_id="run_control",
                document_id="doc_control",
                process_id="first",
                reason="operator cancel",
            )
        )
        self.assertEqual(cancelled.action, "cancel")
        self.assertEqual(cancelled.schedule.cancelled, ["first"])
        self.assertEqual(cancelled.schedule.skipped, [])
        statuses = asyncio.run(
            store.list_statuses(run_id="run_control", document_id="doc_control")
        )
        self.assertEqual(statuses["first"].value, "cancelled")

    def test_sqlite_state_store_persists_outputs_events_and_projections(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            step_script = Path(tmp) / "enrich.py"
            step_script.write_text(
                textwrap.dedent(
                    """
                    import json
                    import sys

                    ctx = json.loads(sys.stdin.read())
                    source = ctx["input"]["values"]["initial"]["source"]
                    print(json.dumps({"values": {"source": source}}))
                    """
                ),
                encoding="utf-8",
            )
            pipeline = PipelineSpec(
                id="persistent_test",
                steps=[
                    ProcessSpec(
                        id="enrich",
                        adapter=AdapterSpec(
                            kind="subprocess",
                            command=[sys.executable, str(step_script)],
                        ),
                    )
                ],
                combines=[CombineSpec(id="bundle", needs=["enrich"])],
            )
            db_path = Path(tmp) / "runtime.db"
            result = asyncio.run(
                PipelineRunner(
                    pipeline,
                    store=SQLiteStateStore(db_path),
                ).run_document(
                    run_id="run_db",
                    document_id="doc_db",
                    values={"source": "db-test.pdf"},
                )
            )

            reopened = SQLiteStateStore(db_path)
            output = asyncio.run(
                reopened.get_output(
                    run_id=result.run_id,
                    document_id=result.document_id,
                    process_id="enrich",
                )
            )
            projection = asyncio.run(
                reopened.get_projection(
                    run_id=result.run_id,
                    document_id=result.document_id,
                    projection_id="bundle",
                )
            )
            events = asyncio.run(
                reopened.list_events(run_id=result.run_id, document_id=result.document_id)
            )
            pipeline_id = asyncio.run(
                reopened.get_document_pipeline_id(
                    run_id=result.run_id,
                    document_id=result.document_id,
                )
            )

        self.assertIsNotNone(output)
        self.assertEqual(output.values["source"], "db-test.pdf")
        self.assertIsNotNone(projection)
        self.assertTrue(projection.complete)
        self.assertEqual(pipeline_id, "persistent_test")
        self.assertIn("document.initialized", [event.type for event in events])
        self.assertIn("process.completed", [event.type for event in events])
