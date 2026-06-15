from __future__ import annotations

# ruff: noqa: E402

import asyncio
import hashlib
import json
import os
import sqlite3
import sys
import tarfile
import tempfile
import textwrap
import unittest
import uuid
from contextlib import closing, redirect_stdout
from datetime import datetime, timedelta, timezone
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
import yaml
from fastapi import FastAPI
from pydantic import ValidationError

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fala import (  # noqa: E402
    RUNTIME_SCHEMA_VERSION,
    AdapterProcessRuntimeWorker,
    AdapterRegistry,
    AdapterSpec,
    ArtifactKindSpec,
    ArtifactRef,
    CapabilitySpec,
    ChildDocumentWaitSpec,
    ClaimedProcess,
    CombineSpec,
    DocumentRelationSpec,
    DocumentTypeSpec,
    ExternalCommandAdapter,
    FileArtifactStore,
    InMemoryStateStore,
    MemoryArtifactStore,
    MemoryQueueTransport,
    OperationTypeSpec,
    OutputDocumentRef,
    PipelineRegistry,
    PipelineRegistryError,
    PipelineRunError,
    PipelineRunner,
    PipelineScheduler,
    PipelineSpec,
    PostgresStateStore,
    ProcessAction,
    ProcessConditionSpec,
    ProcessEvent,
    ProcessExecutionContext,
    ProcessInput,
    ProcessOutput,
    ProcessRuntimeClient,
    ProcessSpec,
    ProcessStatus,
    ProcessSupervisor,
    ProcessWorkerResult,
    QueueResultEnvelope,
    QueueWorkEnvelope,
    RedisQueueTransport,
    ResourceSpec,
    RetryPolicy,
    RunReduceSpec,
    RunStatus,
    RuntimeAccessPolicy,
    RuntimeDocumentInput,
    RuntimeDocumentStatus,
    RuntimeRunInput,
    RuntimeService,
    RuntimeStreamChunk,
    RuntimeWorkerStatus,
    S3ArtifactStore,
    SQLiteQueueTransport,
    SQLiteStateStore,
    ScheduledProcess,
    SpawnDocumentInput,
    StateStore,
    StreamSpec,
    SupervisedWorkerSpec,
    WorkItemPolicy,
    WorkflowPackageSpec,
    WorkflowSecretSpec,
    WorkflowWorkerSpec,
    apply_queue_results,
    assign_queue_work_worker,
    build_package_worker_specs,
    build_workflow_readiness_report,
    build_workflow_registry_index,
    create_artifact_store,
    create_queue_broker_transport,
    create_runtime_router,
    create_runtime_web_app,
    create_state_store,
    default_state_store_target,
    export_claims_to_queue,
    get_scaffold_blueprint,
    list_scaffold_blueprints,
    load_pipeline_yaml,
    read_result_jsonl,
    read_work_jsonl,
    route_runtime_documents_with_report,
    run_queue_work,
    scaffold_blueprint_from_mapping,
    write_jsonl,
)
from fala.cli import main as runtime_cli_main  # noqa: E402
from fala.postgres_store import (  # noqa: E402
    POSTGRES_SCHEMA_SQL,
    POSTGRES_TRY_CLAIM_STATUS_SQL,
)
from fala.state import (  # noqa: E402
    build_runtime_document_state,
    build_runtime_state,
    build_runtime_step_report,
)
from fala.worker_cli import (
    _build_parser as build_runtime_worker_parser,
    main as runtime_worker_cli_main,
)  # noqa: E402


class _FakeRuntimeClient:
    def __init__(self, claim: ClaimedProcess | None) -> None:
        self.claim = claim
        self.outputs: list[dict] = []
        self.statuses: list[dict] = []
        self.renews: list[dict] = []
        self.events: list[dict] = []
        self.worker_heartbeats: list[dict] = []

    async def claim_next(self, **kwargs) -> ClaimedProcess | None:
        self.claim_args = kwargs
        claim = self.claim
        self.claim = None
        return claim

    async def write_output(self, **kwargs) -> dict:
        self.outputs.append(kwargs)
        return {"ok": True}

    async def write_status(self, **kwargs) -> dict:
        self.statuses.append(kwargs)
        status = kwargs.get("status")
        return {"ok": True, "status": status.value if hasattr(status, "value") else status}

    async def append_event(self, **kwargs):
        self.events.append(kwargs)
        return kwargs["event"]

    async def renew_claim(self, **kwargs):
        self.renews.append(kwargs)
        return object()

    async def worker_heartbeat(self, **kwargs):
        self.worker_heartbeats.append(kwargs)
        return kwargs


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


class _FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.put_count = 0

    def put_object(self, **kwargs):
        body = kwargs["Body"]
        data = body.read() if hasattr(body, "read") else body
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = bytes(data)
        self.put_count += 1

    def head_object(self, **kwargs):
        key = (kwargs["Bucket"], kwargs["Key"])
        if key not in self.objects:
            raise KeyError(key)
        return {"ContentLength": len(self.objects[key])}

    def get_object(self, **kwargs):
        key = (kwargs["Bucket"], kwargs["Key"])
        if key not in self.objects:
            raise KeyError(key)
        return {"Body": BytesIO(self.objects[key])}

    def list_objects_v2(self, **kwargs):
        bucket = kwargs["Bucket"]
        prefix = kwargs.get("Prefix") or ""
        contents = [
            {"Key": key, "Size": len(data)}
            for (item_bucket, key), data in sorted(self.objects.items())
            if item_bucket == bucket and key.startswith(prefix)
        ]
        return {"IsTruncated": False, "Contents": contents}

    def delete_objects(self, **kwargs):
        bucket = kwargs["Bucket"]
        deleted = []
        for item in kwargs["Delete"]["Objects"]:
            key = item["Key"]
            self.objects.pop((bucket, key), None)
            deleted.append({"Key": key})
        return {"Deleted": deleted}


class _FakeRedisClient:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.lists: dict[str, list[str]] = {}
        self.sets: dict[str, set[str]] = {}

    def hset(
        self,
        name: str,
        key: str | None = None,
        value: str | None = None,
        **kwargs,
    ):
        mapping = kwargs.get("mapping")
        bucket = self.hashes.setdefault(name, {})
        if mapping is not None:
            for item_key, item_value in mapping.items():
                bucket[str(item_key)] = str(item_value)
            return len(mapping)
        if key is None or value is None:
            raise TypeError("hset requires key/value or mapping")
        bucket[str(key)] = str(value)
        return 1

    def hgetall(self, name: str):
        return dict(self.hashes.get(name, {}))

    def rpush(self, name: str, *values: str):
        queue = self.lists.setdefault(name, [])
        queue.extend(str(value) for value in values)
        return len(queue)

    def lpop(self, name: str):
        queue = self.lists.setdefault(name, [])
        if not queue:
            return None
        return queue.pop(0)

    def sadd(self, name: str, *values: str):
        target = self.sets.setdefault(name, set())
        before = len(target)
        target.update(str(value) for value in values)
        return len(target) - before

    def smembers(self, name: str):
        return set(self.sets.get(name, set()))


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
                    document_types:
                      - id: contract_pdf
                        title: Contract PDF
                        media_types: ["application/pdf"]
                        extensions: [".pdf"]
                      - id: contract_page
                        title: Contract page
                        media_types: ["application/pdf"]
                        extensions: [".pdf"]
                    artifact_kinds:
                      - id: extracted_text
                        title: Extracted text
                        media_types: ["text/plain"]
                    capabilities:
                      - id: extract_text
                        title: Extract text
                        accepts_document_types: [contract_pdf]
                        emits_document_types: [contract_page]
                        emits_artifact_kinds: [extracted_text]
                    secrets:
                      - id: openai_api_key
                        env_var: OPENAI_API_KEY
                        kubernetes_secret_name: fala-openai
                    pipelines:
                      - demo.yaml
                    workers:
                      - id: first_worker
                        capabilities: [extract_text]
                        pipeline: packaged_demo
                        process: first
                        adapter_kind: queue
                        command: ["python", "steps/first.py"]
                        cwd: "."
                        secrets: [openai_api_key]
                        sandbox:
                          read_only_root_filesystem: true
                          allow_privilege_escalation: false
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
                        capability: extract_text
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
            self.assertEqual(registry.packages()[0].document_types[0].id, "contract_pdf")
            self.assertEqual(registry.packages()[0].artifact_kinds[0].id, "extracted_text")
            self.assertEqual(registry.packages()[0].capabilities[0].id, "extract_text")
            self.assertEqual(registry.packages()[0].secrets[0].id, "openai_api_key")
            self.assertEqual(
                registry.packages()[0].workers[0].secrets,
                ["openai_api_key"],
            )
            self.assertEqual(
                registry.packages()[0].workers[0].pipeline_id,
                "packaged_demo",
            )
            self.assertEqual(registry.packages()[0].workers[0].process_id, "first")
            self.assertEqual(registry.packages()[0].workers[0].capabilities, ["extract_text"])
            self.assertEqual(registry.get("packaged_demo").steps[0].capability, "extract_text")
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
            self.assertEqual(validate["packages"][0]["document_types"][0]["id"], "contract_pdf")
            self.assertEqual(validate["packages"][0]["artifact_kinds"][0]["id"], "extracted_text")
            self.assertEqual(validate["packages"][0]["capabilities"][0]["id"], "extract_text")
            self.assertEqual(validate["packages"][0]["secrets"][0]["env_var"], "OPENAI_API_KEY")
            self.assertEqual(validate["packages"][0]["workers"][0]["id"], "first_worker")
            self.assertEqual(validate["packages"][0]["workers"][0]["capabilities"], ["extract_text"])
            self.assertEqual(validate["packages"][0]["workers"][0]["secrets"], ["openai_api_key"])
            self.assertEqual(validate["pipelines"][0]["package_id"], "demo_package")
            self.assertEqual(validate["pipelines"][0]["steps"][0]["capability"], "extract_text")

            contract = registry.pipeline_contract("packaged_demo")
            self.assertEqual(contract["pipeline_id"], "packaged_demo")
            self.assertEqual(contract["package_id"], "demo_package")
            self.assertEqual(contract["document_types"][0]["id"], "contract_pdf")
            self.assertEqual(contract["steps"][0]["capability"]["id"], "extract_text")
            self.assertEqual(
                contract["steps"][0]["input_document_types"][0]["id"],
                "contract_pdf",
            )
            self.assertEqual(
                contract["steps"][0]["emitted_document_types"][0]["id"],
                "contract_page",
            )
            self.assertEqual(
                contract["steps"][0]["emitted_artifact_kinds"][0]["id"],
                "extracted_text",
            )

            cli_contract = _run_cli(
                "--pipeline-dir",
                str(root),
                "contract",
                "packaged_demo",
            )
            self.assertTrue(cli_contract["ok"])
            self.assertEqual(
                cli_contract["contract"]["steps"][0]["capability"]["id"],
                "extract_text",
            )

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
            self.assertEqual(commands["workers"][0]["capabilities"], ["extract_text"])
            self.assertIn("--package-worker", commands["workers"][0]["argv"])
            self.assertIn("first_worker", commands["workers"][0]["shell"])

    def test_runtime_cli_lints_python_step_contracts_against_pipeline(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "demo_package"
            package_dir.mkdir()
            (package_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: demo_package
                    artifact_kinds:
                      - id: text
                      - id: enriched
                    capabilities:
                      - id: ingest_text
                        emits_artifact_kinds: [text]
                      - id: enrich_text
                        accepts_artifact_kinds: [text]
                        emits_artifact_kinds: [enriched]
                    pipelines:
                      - demo.yaml
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "demo.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: demo
                    steps:
                      - id: ingest
                        capability: ingest_text
                        adapter:
                          kind: subprocess
                          command: ["python", "ingest.py"]
                      - id: enrich
                        capability: enrich_text
                        needs: [ingest]
                        adapter:
                          kind: subprocess
                          command: ["python", "enrich.py"]
                    """
                ).strip(),
                encoding="utf-8",
            )
            (root / "demo_contracts.py").write_text(
                textwrap.dedent(
                    """
                    from fala.sdk import JsonArtifact, JsonNeed, StepContract

                    STEP_CONTRACTS = [
                        StepContract(
                            process_id="ingest",
                            outputs={"text": JsonArtifact("text", "text.txt")},
                        ),
                        StepContract(
                            process_id="enrich",
                            needs={"text": JsonNeed("ingest", "text")},
                            outputs={"enriched": JsonArtifact("enriched", "enriched.json")},
                        ),
                    ]

                    BROKEN_CONTRACTS = [
                        StepContract(
                            process_id="enrich",
                            needs={"text": JsonNeed("ingest", "missing_text")},
                            outputs={"enriched": JsonArtifact("wrong", "enriched.json")},
                        ),
                    ]
                    """
                ).strip(),
                encoding="utf-8",
            )

            ok = _run_cli(
                "--pipeline-dir",
                str(root),
                "contract-lint",
                "--pipeline",
                "demo",
                "--python-path",
                str(root),
                "--contract",
                "demo_contracts:STEP_CONTRACTS",
            )
            code, broken = _run_cli_raw(
                "--pipeline-dir",
                str(root),
                "contract-lint",
                "--pipeline",
                "demo",
                "--python-path",
                str(root),
                "--contract",
                "demo_contracts:BROKEN_CONTRACTS",
                "--allow-missing",
            )
            auto = _run_cli(
                "--pipeline-dir",
                str(root),
                "contract-lint",
                "--pipeline",
                "demo",
                "--python-path",
                str(root),
            )
            doctor_code, doctor = _run_cli_raw(
                "--pipeline-dir",
                str(root),
                "package-doctor",
                "--python-path",
                str(root),
            )

        self.assertTrue(ok["ok"])
        self.assertEqual(ok["contract_count"], 2)
        self.assertEqual(ok["issue_count"], 0)
        self.assertTrue(auto["ok"])
        self.assertIn("demo_contracts:STEP_CONTRACTS", auto["discovery"]["refs"])
        self.assertEqual(doctor_code, 1)
        package = doctor["readiness"]["packages"][0]
        self.assertEqual(package["step_contract_count"], 2)
        self.assertEqual(package["step_contract_issue_count"], 0)
        self.assertEqual(package["step_contract_lints"][0]["issue_count"], 0)
        self.assertEqual(code, 1)
        self.assertFalse(broken["ok"])
        issue_codes = {issue["code"] for issue in broken["issues"]}
        self.assertIn("need_artifact_not_emitted", issue_codes)
        self.assertIn("output_artifact_not_emitted", issue_codes)

    def test_runtime_cli_writes_and_verifies_step_replay_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            artifact_path = root / "input.json"
            artifact_path.write_text('{"name": "acetone"}\n', encoding="utf-8")
            manifest_path = root / "step_run_manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "schema": "fala.step_run_manifest.v1",
                        "pipeline_id": "demo",
                        "run_id": "run_bundle",
                        "document_id": "doc_1",
                        "process_id": "enrich",
                        "context": {
                            "run_id": "run_bundle",
                            "document_id": "doc_1",
                            "process_id": "enrich",
                            "input": {
                                "artifacts": [
                                    {
                                        "kind": "substances",
                                        "uri": artifact_path.resolve().as_uri(),
                                    }
                                ],
                                "values": {
                                    "needs": {
                                        "extract": {
                                            "artifacts": [
                                                {
                                                    "kind": "substances",
                                                    "uri": artifact_path.resolve().as_uri(),
                                                }
                                            ]
                                        }
                                    }
                                },
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            bundle_path = root / "step-bundle.tar.gz"
            bundled = _run_cli(
                "step-bundle",
                "--manifest",
                str(manifest_path),
                "--output",
                str(bundle_path),
                "--cwd",
                str(root),
                "--",
                "python",
                "-c",
                "import json, sys; print(json.dumps({'values': {'ok': True}}))",
            )

            self.assertTrue(bundled["ok"])
            self.assertEqual(str(bundle_path.resolve()), bundled["output"])
            self.assertEqual(bundled["artifact_count"], 1)
            with tarfile.open(bundle_path, "r:gz") as archive:
                names = set(archive.getnames())
                prefix = bundled["bundle_name"]
                self.assertIn(f"{prefix}/bundle-manifest.json", names)
                self.assertIn(f"{prefix}/step_run_manifest.bundle.json", names)
                self.assertIn(f"{prefix}/replay.py", names)
                replay_manifest = json.loads(
                    archive.extractfile(
                        f"{prefix}/step_run_manifest.bundle.json"
                    ).read().decode("utf-8")
                )
                replay_uri = replay_manifest["context"]["input"]["artifacts"][0]["uri"]
                self.assertTrue(replay_uri.startswith("artifacts/"))
            verified = _run_cli("step-bundle-verify", str(bundle_path))
            self.assertTrue(verified["ok"])
            self.assertEqual(verified["artifact_count"], 1)

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

    def test_workflow_package_rejects_invalid_capability_references(self) -> None:
        invalid_packages = [
            (
                lambda: WorkflowPackageSpec(
                    id="pkg",
                    pipelines=["demo.yaml"],
                    artifact_kinds=[ArtifactKindSpec(id="text")],
                    capabilities=[
                        CapabilitySpec(
                            id="extract",
                            accepts_document_types=["missing_doc"],
                            emits_artifact_kinds=["text"],
                        )
                    ],
                ),
                ValidationError,
                "unknown id",
            ),
            (
                lambda: WorkflowPackageSpec(
                    id="pkg",
                    pipelines=["demo.yaml"],
                    capabilities=[CapabilitySpec(id="extract")],
                    workers=[
                        WorkflowWorkerSpec(
                            id="worker",
                            capabilities=["missing_capability"],
                            pipeline_id="demo",
                            command=["worker"],
                        )
                    ],
                ),
                ValidationError,
                "unknown id",
            ),
            (
                lambda: WorkflowPackageSpec(
                    id="pkg",
                    pipelines=["demo.yaml"],
                    document_types=[DocumentTypeSpec(id="page_document")],
                    capabilities=[
                        CapabilitySpec(
                            id="split",
                            emits_document_types=["missing_document"],
                        )
                    ],
                ),
                ValidationError,
                "unknown id",
            ),
            (
                lambda: WorkflowPackageSpec(
                    id="pkg",
                    pipelines=["demo.yaml"],
                    document_types=[DocumentTypeSpec(id="source_document")],
                    document_relations=[
                        DocumentRelationSpec(
                            id="page",
                            source_document_types=["source_document"],
                            target_document_types=["missing_page"],
                        )
                    ],
                ),
                ValidationError,
                "unknown id",
            ),
            (
                lambda: WorkflowPackageSpec(
                    id="pkg",
                    pipelines=["demo.yaml"],
                    operation_types=[OperationTypeSpec(id="extract")],
                    capabilities=[
                        CapabilitySpec(
                            id="extract_text",
                            operation_type="missing_operation",
                        )
                    ],
                ),
                ValidationError,
                "unknown id",
            ),
            (
                lambda: WorkflowPackageSpec(
                    id="pkg",
                    pipelines=["demo.yaml"],
                    capabilities=[CapabilitySpec(id="extract")],
                    workers=[
                        WorkflowWorkerSpec(
                            id="worker",
                            pipeline_id="demo",
                            command=["worker"],
                            secrets=["missing_secret"],
                        )
                    ],
                ),
                ValidationError,
                "unknown id",
            ),
            (
                lambda: WorkflowPackageSpec(
                    id="pkg",
                    pipelines=["demo.yaml"],
                    secrets=[
                        WorkflowSecretSpec(
                            id="api_key",
                            env_var="API_KEY",
                        )
                    ],
                    workers=[
                        WorkflowWorkerSpec(
                            id="worker",
                            pipeline_id="demo",
                            command=["worker"],
                            env={"API_KEY": "inline-secret"},
                            secrets=["api_key"],
                        )
                    ],
                ),
                ValidationError,
                "env overrides secret env var",
            ),
            (
                lambda: WorkflowPackageSpec(
                    id="pkg",
                    pipelines=["demo.yaml"],
                    capabilities=[
                        CapabilitySpec(
                            id="extract",
                            output_schema={"type": 42},
                        )
                    ],
                ),
                ValidationError,
                "output_schema JSON schema is invalid",
            ),
            (
                lambda: WorkflowPackageSpec(
                    id="pkg",
                    pipelines=["demo.yaml"],
                    capabilities=[
                        CapabilitySpec(
                            id="extract",
                            emits_streams=[
                                StreamSpec(
                                    stream_id="pages",
                                    emits_artifact_kinds=["missing_text"],
                                )
                            ],
                        )
                    ],
                ),
                ValidationError,
                "unknown id",
            ),
            (
                lambda: WorkflowPackageSpec(
                    id="pkg",
                    pipelines=["demo.yaml"],
                    capabilities=[
                        CapabilitySpec(
                            id="extract",
                            emits_streams=[
                                StreamSpec(
                                    stream_id="pages",
                                    value_schema={"type": 42},
                                )
                            ],
                        )
                    ],
                ),
                ValidationError,
                "stream 'pages' value_schema JSON schema is invalid",
            ),
        ]
        for factory, error_type, expected_error in invalid_packages:
            with self.subTest(expected_error=expected_error):
                with self.assertRaisesRegex(error_type, expected_error):
                    factory()

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "demo_package"
            package_dir.mkdir()
            (package_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: demo_package
                    capabilities:
                      - id: known_capability
                    pipelines:
                      - demo.yaml
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
                        capability: missing_capability
                        adapter:
                          kind: queue
                          queue: demo.first
                    """
                ).strip(),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(PipelineRegistryError, "unknown capability"):
                PipelineRegistry.from_directory(root)

    def test_workflow_package_rejects_root_capability_without_document_type(self) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="pkg",
                pipelines=["demo.yaml"],
                document_types=[DocumentTypeSpec(id="generic_document")],
                artifact_kinds=[ArtifactKindSpec(id="text")],
                capabilities=[
                    CapabilitySpec(id="extract", emits_artifact_kinds=["text"]),
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="demo",
                steps=[
                    ProcessSpec(
                        id="extract",
                        capability="extract",
                        adapter=AdapterSpec(kind="queue", queue="demo.extract"),
                    )
                ],
            ),
            package_id="pkg",
        )

        with self.assertRaisesRegex(
            PipelineRegistryError,
            "root process 'extract'.*does not accept any document type",
        ):
            registry.validate_package_workers("pkg")

    def test_workflow_package_rejects_artifact_kind_flow_mismatch(self) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="pkg",
                pipelines=["demo.yaml"],
                document_types=[DocumentTypeSpec(id="generic_document")],
                artifact_kinds=[
                    ArtifactKindSpec(id="text"),
                    ArtifactKindSpec(id="image"),
                ],
                capabilities=[
                    CapabilitySpec(
                        id="extract",
                        accepts_document_types=["generic_document"],
                        emits_artifact_kinds=["text"],
                    ),
                    CapabilitySpec(
                        id="detect_objects",
                        accepts_artifact_kinds=["image"],
                    ),
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="demo",
                steps=[
                    ProcessSpec(
                        id="extract",
                        capability="extract",
                        adapter=AdapterSpec(kind="queue", queue="demo.extract"),
                    ),
                    ProcessSpec(
                        id="detect_objects",
                        capability="detect_objects",
                        needs=["extract"],
                        adapter=AdapterSpec(kind="queue", queue="demo.detect"),
                    ),
                ],
            ),
            package_id="pkg",
        )

        with self.assertRaisesRegex(
            PipelineRegistryError,
            "does not accept artifacts emitted by its needs",
        ):
            registry.validate_package_workers("pkg")

    def test_workflow_package_rejects_step_config_not_matching_capability_schema(self) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="pkg",
                pipelines=["demo.yaml"],
                capabilities=[
                    CapabilitySpec(
                        id="extract",
                        config_schema={
                            "type": "object",
                            "required": ["model"],
                            "properties": {"model": {"type": "string"}},
                            "additionalProperties": False,
                        },
                    )
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="demo",
                steps=[
                    ProcessSpec(
                        id="extract",
                        capability="extract",
                        config={"temperature": 0.2},
                        adapter=AdapterSpec(kind="queue", queue="demo.extract"),
                    )
                ],
            ),
            package_id="pkg",
        )

        with self.assertRaisesRegex(
            PipelineRegistryError,
            "process 'extract' config.*'model' is a required property",
        ):
            registry.validate_package_workers("pkg")

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
            lambda: RetryPolicy(
                retry_error_kinds=["transient_io"],
                terminal_error_kinds=["transient_io"],
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
        self.assertIsNone(AdapterSpec(kind="manual").queue)

        invalid_specs = [
            {"kind": "subprocess"},
            {"kind": "subprocess", "command": [], "queue": "workflow.extract"},
            {"kind": "http"},
            {"kind": "http", "url": "http://worker.local", "env": {"A": "B"}},
            {"kind": "queue"},
            {"kind": "queue", "queue": "workflow.extract", "command": ["worker"]},
            {"kind": "manual", "queue": "workflow.review"},
            {"kind": "manual", "command": ["worker"]},
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
                    capability="enrich_document",
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
        self.assertEqual(document.steps[1].capability, "enrich_document")
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

    def test_runtime_step_report_tracks_progress_and_blockers(self) -> None:
        pipeline = PipelineSpec(
            id="report_flow",
            steps=[
                ProcessSpec(id="first", adapter=AdapterSpec(kind="queue", queue="first")),
                ProcessSpec(
                    id="second",
                    needs=["first"],
                    adapter=AdapterSpec(kind="queue", queue="second"),
                ),
                ProcessSpec(
                    id="third",
                    needs=["second"],
                    adapter=AdapterSpec(kind="queue", queue="third"),
                ),
            ],
        )
        output = ProcessOutput(values={"text": "ok"})
        document = build_runtime_document_state(
            document_id="doc.pdf",
            pipeline_id="report_flow",
            pipeline=pipeline,
            statuses={
                "first": ProcessStatus.completed,
                "second": ProcessStatus.queued,
                "third": ProcessStatus.waiting,
            },
            claims={},
            outputs={"first": output},
            projections={},
            events=[],
        )
        report = build_runtime_step_report(
            build_runtime_state(run_id="run_report", documents=[document])
        )

        self.assertEqual(report.summary.document_count, 1)
        self.assertEqual(report.summary.process_count, 3)
        self.assertEqual(report.summary.terminal_process_count, 1)
        self.assertEqual(report.summary.completed_process_count, 1)
        self.assertEqual(report.summary.blocked_process_count, 1)
        self.assertAlmostEqual(report.summary.progress, 1 / 3)
        self.assertEqual(report.documents[0].progress, 1 / 3)
        self.assertEqual([step.process_id for step in report.steps], ["first", "second", "third"])
        self.assertEqual(report.steps[0].status_category, "terminal")
        self.assertEqual(report.steps[1].blocked_by, [])
        self.assertEqual(report.steps[2].blocked_by, ["second"])
        self.assertTrue(report.steps[2].is_blocked)

    def test_runtime_step_report_is_available_from_api_and_client(self) -> None:
        pipeline = PipelineSpec(
            id="report_flow",
            steps=[
                ProcessSpec(id="first", adapter=AdapterSpec(kind="queue", queue="first")),
                ProcessSpec(
                    id="second",
                    needs=["first"],
                    adapter=AdapterSpec(kind="queue", queue="second"),
                ),
            ],
        )
        service = RuntimeService(
            registry=PipelineRegistry([pipeline]),
            store=InMemoryStateStore(),
        )
        app = FastAPI()
        app.include_router(create_runtime_router(service), prefix="/api")

        async def run_client() -> None:
            async with ProcessRuntimeClient(
                "http://runtime.test",
                transport=httpx.ASGITransport(app=app),
            ) as client:
                await client.initialize_document(
                    run_id="run_report",
                    document_id="doc.pdf",
                    pipeline_id="report_flow",
                    values={"source": "doc.pdf"},
                )
                initial = await client.get_step_report(run_id="run_report")
                self.assertEqual(initial.summary.process_count, 2)
                self.assertEqual(initial.summary.status_counts["queued"], 1)
                self.assertEqual(initial.summary.status_counts["waiting"], 1)
                self.assertEqual(initial.steps[1].blocked_by, ["first"])

                claimed = await client.claim_next(
                    run_id="run_report",
                    pipeline_id="report_flow",
                    worker_id="worker-1",
                )
                self.assertIsNotNone(claimed)
                running = await client.get_step_report(run_id="run_report")
                self.assertEqual(running.steps[0].status, ProcessStatus.running)
                self.assertTrue(running.steps[0].is_active)
                self.assertEqual(running.steps[0].worker_id, "worker-1")
                self.assertEqual(running.summary.active_process_count, 1)

                await client.write_output(
                    run_id="run_report",
                    document_id="doc.pdf",
                    process_id="first",
                    output=ProcessOutput(values={"ok": True}),
                    pipeline_id="report_flow",
                    worker_id="worker-1",
                )
                after_output = await client.get_step_report(run_id="run_report")
                self.assertEqual(after_output.steps[0].status, ProcessStatus.completed)
                self.assertEqual(after_output.steps[1].status, ProcessStatus.queued)
                self.assertEqual(after_output.steps[1].blocked_by, [])
                self.assertAlmostEqual(after_output.summary.progress, 0.5)

        asyncio.run(run_client())

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

            paused = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "control-run",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--action",
                "pause",
                "--reason",
                "operator pause",
            )
            self.assertEqual(paused["run"]["status"], "paused")
            paused_claim = _run_cli(
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
            self.assertIsNone(paused_claim["claim"])
            resumed = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "control-run",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--action",
                "resume",
                "--reason",
                "operator resume",
            )
            self.assertEqual(resumed["run"]["status"], "queued")

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

            cancelled = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "control-run",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli",
                "--action",
                "cancel",
                "--reason",
                "operator cancel",
            )
            self.assertEqual(cancelled["run"]["status"], "cancelled")
            self.assertEqual(cancelled["run"]["outcome"], "cancelled")
            cancelled_claim = _run_cli(
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
            self.assertIsNone(cancelled_claim["claim"])
            retention = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "run-retention",
                "--db",
                str(db_path),
                "--before",
                "2999-01-01T00:00:00+00:00",
                "--status",
                "cancelled",
            )
            self.assertTrue(retention["retention"]["dry_run"])
            self.assertEqual(retention["retention"]["candidate_count"], 1)
            self.assertEqual(
                retention["retention"]["runs"][0]["run_id"],
                "run_cli",
            )

            artifact_store = FileArtifactStore(root / "artifact-store")
            source_path = root / "cli-source.txt"
            orphan_path = root / "cli-orphan.txt"
            source_path.write_text("source", encoding="utf-8")
            orphan_path.write_text("orphan", encoding="utf-8")
            source_ref = artifact_store.put_file(kind="source", path=source_path)
            orphan_ref = artifact_store.put_file(kind="unused", path=orphan_path)
            _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "init-document",
                "--db",
                str(db_path),
                "--pipeline",
                "demo",
                "--run-id",
                "run_cli_gc",
                "--document-id",
                "gc-doc",
                "--artifact",
                f"source={source_ref.uri}",
            )
            gc_dry_run = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "artifact-gc",
                "--db",
                str(db_path),
                "--artifact-store-root",
                str(artifact_store.root),
            )
            self.assertTrue(gc_dry_run["artifact_gc"]["dry_run"])
            self.assertEqual(gc_dry_run["artifact_gc"]["orphaned_blob_count"], 1)
            gc_deleted = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "artifact-gc",
                "--db",
                str(db_path),
                "--artifact-store-root",
                str(artifact_store.root),
                "--delete",
            )
            self.assertEqual(gc_deleted["artifact_gc"]["deleted_blob_count"], 1)
            self.assertTrue(artifact_store.resolve(source_ref).exists())
            with self.assertRaises(FileNotFoundError):
                artifact_store.resolve(orphan_ref)

    def test_runtime_cli_claim_honors_resource_requirements(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipeline_dir = root / "pipelines"
            pipeline_dir.mkdir()
            (pipeline_dir / "resource.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: resource_cli
                    steps:
                      - id: ocr
                        capability: ocr_pdf
                        resources:
                          gpu_count: 1
                          memory_mb: 2048
                          labels: [cuda]
                          units:
                            ocr_slots: 2
                        adapter:
                          kind: queue
                          queue: resource.ocr
                    """
                ),
                encoding="utf-8",
            )
            db_path = root / "runtime.db"

            init = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "init-document",
                "--db",
                str(db_path),
                "--pipeline",
                "resource_cli",
                "--run-id",
                "run_resource_cli",
                "--document-id",
                "doc.pdf",
            )
            self.assertEqual(init["schedule"]["queued"][0]["resources"]["gpu_count"], 1)

            blocked = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "claim",
                "--db",
                str(db_path),
                "--pipeline",
                "resource_cli",
                "--run-id",
                "run_resource_cli",
                "--worker-id",
                "worker-cli",
                "--adapter-kind",
                "queue",
                "--capability",
                "ocr_pdf",
                "--gpu-count",
                "1",
            )
            self.assertIsNone(blocked["claim"])

            claim = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "claim",
                "--db",
                str(db_path),
                "--pipeline",
                "resource_cli",
                "--run-id",
                "run_resource_cli",
                "--worker-id",
                "worker-cli",
                "--adapter-kind",
                "queue",
                "--capability",
                "ocr_pdf",
                "--gpu-count",
                "1",
                "--memory-mb",
                "4096",
                "--resource-label",
                "cuda",
                "--resource-unit",
                "ocr_slots=2",
            )
            self.assertEqual(claim["claim"]["process"]["id"], "ocr")
            self.assertEqual(claim["claim"]["process"]["resources"]["labels"], ["cuda"])

    def test_runtime_cli_create_run_accepts_resource_pool_quotas(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipeline_dir = root / "pipelines"
            pipeline_dir.mkdir()
            (pipeline_dir / "quota.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: quota_cli
                    steps:
                      - id: ocr
                        capability: ocr_pdf
                        resource_pool: gpu_pool
                        resources:
                          gpu_count: 1
                          memory_mb: 1024
                        adapter:
                          kind: queue
                          queue: quota.ocr
                    """
                ),
                encoding="utf-8",
            )
            db_path = root / "runtime.db"
            created = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "create-run",
                "--db",
                str(db_path),
                "--pipeline",
                "quota_cli",
                "--run-id",
                "run_quota_cli",
                "--document",
                "doc_a=file:///tmp/a.pdf",
                "--document",
                "doc_b=file:///tmp/b.pdf",
                "--resource-pool",
                "gpu_pool.gpu_count=1",
                "--resource-pool",
                "gpu_pool.memory_mb=1024",
            )
            self.assertTrue(created["ok"])
            self.assertEqual(
                created["run"]["config"]["resource_pools"]["gpu_pool"]["gpu_count"],
                1,
            )

            first = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "claim",
                "--db",
                str(db_path),
                "--pipeline",
                "quota_cli",
                "--run-id",
                "run_quota_cli",
                "--worker-id",
                "gpu-worker",
                "--adapter-kind",
                "queue",
                "--capability",
                "ocr_pdf",
                "--gpu-count",
                "1",
                "--memory-mb",
                "4096",
            )
            self.assertEqual(first["claim"]["document_id"], "doc_a")
            self.assertEqual(first["claim"]["process"]["resource_pool"], "gpu_pool")

            blocked = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "claim",
                "--db",
                str(db_path),
                "--pipeline",
                "quota_cli",
                "--run-id",
                "run_quota_cli",
                "--worker-id",
                "gpu-worker",
                "--adapter-kind",
                "queue",
                "--capability",
                "ocr_pdf",
                "--gpu-count",
                "1",
                "--memory-mb",
                "4096",
            )
            self.assertIsNone(blocked["claim"])

            metrics = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "queue-metrics",
                "--db",
                str(db_path),
                "--run-id",
                "run_quota_cli",
            )
            self.assertEqual(metrics["metrics"]["resource_blocked_count"], 1)
            self.assertEqual(metrics["metrics"]["resource_pools"][0]["id"], "gpu_pool")
            self.assertEqual(metrics["metrics"]["resource_pools"][0]["used"]["gpu_count"], 1)

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
            self.assertTrue((package_dir / "run-input.example.yaml").exists())
            self.assertTrue((package_dir / "source-list.example.csv").exists())
            self.assertTrue((package_dir / "incoming" / "sample.bin").exists())
            self.assertTrue((package_dir / "README.scaffold.md").exists())
            self.assertTrue((package_dir / "Makefile").exists())
            self.assertTrue((package_dir / "steps" / "ingest.py").exists())
            self.assertTrue(
                (
                    package_dir
                    / "contracts"
                    / "documents"
                    / "generic_document.values.schema.yaml"
                ).exists()
            )
            self.assertTrue(
                (
                    package_dir
                    / "contracts"
                    / "capabilities"
                    / "ingest_document.output.schema.yaml"
                ).exists()
            )
            self.assertIn(
                "run-input.example.yaml",
                [Path(path).name for path in scaffold["created"]],
            )
            self.assertIn(
                "generic_document.values.schema.yaml",
                [Path(path).name for path in scaffold["created"]],
            )
            self.assertIn(
                "source-list.example.csv",
                [Path(path).name for path in scaffold["created"]],
            )
            run_input_text = (package_dir / "run-input.example.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn("pipeline_id: demo_flow", run_input_text)
            self.assertIn("document_type: generic_document", run_input_text)
            self.assertIn("media_type: application/octet-stream", run_input_text)
            readme_text = (package_dir / "README.scaffold.md").read_text(
                encoding="utf-8"
            )
            self.assertIn(
                "validate-run --run-input run-input.example.yaml",
                readme_text,
            )
            self.assertIn("package-doctor", readme_text)
            self.assertIn("discover-documents", readme_text)
            self.assertIn("run-until-idle", readme_text)
            self.assertIn("sync-contracts", readme_text)
            self.assertIn("--contract-dir contracts", readme_text)
            self.assertIn("## Contract Surface", readme_text)
            self.assertIn("## Worker Guidance", readme_text)
            self.assertIn("Document types: generic_document", readme_text)
            self.assertIn("| ingest | Ingest | Implement ingest work", readme_text)
            self.assertIn("## Step Policy", readme_text)
            self.assertIn("| ingest | subprocess | ingest_document | - | 0 | - | default |", readme_text)
            self.assertIn("editable copies live in `contracts/`", readme_text)
            ingest_step_text = (package_dir / "steps" / "ingest.py").read_text(
                encoding="utf-8"
            )
            self.assertIn("WORKER_GUIDANCE", ingest_step_text)
            self.assertIn("'operation_type': 'ingest'", ingest_step_text)
            self.assertIn("'artifact_kind': 'ingest_output'", ingest_step_text)
            self.assertIn('"worker_guidance": WORKER_GUIDANCE', ingest_step_text)
            makefile_text = (package_dir / "Makefile").read_text(
                encoding="utf-8"
            )
            self.assertIn("FALA ?= fala", makefile_text)
            self.assertIn("bootstrap: validate doctor validate-run source-list", makefile_text)
            self.assertIn("run-local:", makefile_text)
            self.assertIn("--adapter-kind subprocess", makefile_text)
            self.assertIn("worker-commands:", makefile_text)
            source_list_text = (package_dir / "source-list.example.csv").read_text(
                encoding="utf-8"
            )
            self.assertIn("value.source", source_list_text)
            self.assertIn("incoming/sample.bin", source_list_text)

            validate = _run_cli(
                "--pipeline-dir",
                str(root),
                "validate",
                "--json",
                "--check-commands",
            )
            self.assertTrue(validate["ok"])
            self.assertEqual(validate["packages"][0]["id"], "demo_package")
            self.assertEqual(
                validate["packages"][0]["document_types"][0]["id"],
                "generic_document",
            )
            self.assertEqual(
                validate["packages"][0]["artifact_kinds"][0]["id"],
                "ingest_output",
            )
            self.assertEqual(
                validate["packages"][0]["capabilities"][0]["id"],
                "ingest_document",
            )
            self.assertEqual(validate["pipelines"][0]["steps"][0]["id"], "ingest")
            self.assertEqual(
                validate["pipelines"][0]["steps"][0]["capability"],
                "ingest_document",
            )
            doctor = _run_cli("--pipeline-dir", str(root), "package-doctor")
            self.assertTrue(doctor["ok"])
            self.assertEqual(doctor["readiness"]["warning_count"], 0)
            sample_files = doctor["readiness"]["packages"][0]["sample_files"]
            self.assertTrue(sample_files["run_input_example_valid"])
            self.assertTrue(sample_files["source_list_example_valid"])
            self.assertTrue(sample_files["source_list_local_sources_present"])
            self.assertTrue(sample_files["makefile"])
            sample_run_input = _run_cli(
                "--pipeline-dir",
                str(root),
                "validate-run",
                "--run-input",
                str(package_dir / "run-input.example.yaml"),
            )
            self.assertTrue(sample_run_input["ok"])
            self.assertEqual(sample_run_input["pipeline_id"], "demo_flow")
            self.assertEqual(sample_run_input["document_count"], 1)
            self.assertEqual(
                sample_run_input["documents"][0]["document_type"],
                "generic_document",
            )
            self.assertEqual(
                sample_run_input["documents"][0]["value_keys"],
                ["source"],
            )
            source_list_manifest = _run_cli(
                "discover-documents",
                "--source-list",
                str(package_dir / "source-list.example.csv"),
                "--pipeline",
                "demo_flow",
                "--run-id",
                "run_source_list_sample",
            )
            self.assertEqual(source_list_manifest["pipeline_id"], "demo_flow")
            self.assertEqual(
                source_list_manifest["documents"][0]["document_type"],
                "generic_document",
            )
            self.assertEqual(
                source_list_manifest["documents"][0]["values"]["source"],
                source_list_manifest["documents"][0]["document_id"],
            )
            self.assertTrue(
                Path(
                    source_list_manifest["documents"][0]["source_uri"].removeprefix(
                        "file://"
                    )
                ).is_file()
            )
            source_manifest_path = root / "run-input.from-source-list.json"
            source_manifest_path.write_text(
                json.dumps(source_list_manifest),
                encoding="utf-8",
            )
            source_list_preview = _run_cli(
                "--pipeline-dir",
                str(root),
                "validate-run",
                "--run-input",
                str(source_manifest_path),
            )
            self.assertTrue(source_list_preview["ok"])

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
            self.assertEqual(
                document["outputs"]["ingest"]["artifacts"][0]["kind"],
                "ingest_output",
            )
            self.assertTrue(document["projections"]["workflow_result"]["complete"])

    def test_runtime_cli_builtin_blueprints_scaffold_doctor_clean_samples(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            for summary in list_scaffold_blueprints():
                blueprint_id = summary["id"]
                package_id = f"{blueprint_id}_package"
                pipeline_id = f"{blueprint_id}_flow"
                package_dir = root / blueprint_id
                scaffold = _run_cli(
                    "scaffold",
                    "--output-dir",
                    str(package_dir),
                    "--package-id",
                    package_id,
                    "--pipeline-id",
                    pipeline_id,
                    "--blueprint",
                    blueprint_id,
                    "--adapter-kind",
                    "subprocess",
                )
                self.assertEqual(scaffold["blueprint"], blueprint_id)
                self.assertTrue((package_dir / "run-input.example.yaml").is_file())
                self.assertTrue((package_dir / "source-list.example.csv").is_file())
                incoming = package_dir / "incoming"
                self.assertEqual(len(list(incoming.iterdir())), 1)

                sample = _run_cli(
                    "--pipeline-dir",
                    str(root),
                    "validate-run",
                    "--run-input",
                    str(package_dir / "run-input.example.yaml"),
                )
                self.assertTrue(sample["ok"])
                self.assertEqual(sample["pipeline_id"], pipeline_id)
                self.assertEqual(sample["document_count"], 1)

                discovered = _run_cli(
                    "discover-documents",
                    "--source-list",
                    str(package_dir / "source-list.example.csv"),
                    "--pipeline",
                    pipeline_id,
                    "--run-id",
                    f"run_{blueprint_id}_source_list",
                )
                self.assertEqual(discovered["pipeline_id"], pipeline_id)
                self.assertEqual(len(discovered["documents"]), 1)
                self.assertTrue(
                    Path(
                        discovered["documents"][0]["source_uri"].removeprefix(
                            "file://"
                        )
                    ).is_file()
                )
                manifest_path = package_dir / "run-input.from-source-list.json"
                manifest_path.write_text(json.dumps(discovered), encoding="utf-8")
                preview = _run_cli(
                    "--pipeline-dir",
                    str(root),
                    "validate-run",
                    "--run-input",
                    str(manifest_path),
                )
                self.assertTrue(preview["ok"])

            validate = _run_cli(
                "--pipeline-dir",
                str(root),
                "validate",
                "--json",
                "--check-commands",
            )
            self.assertTrue(validate["ok"])
            self.assertEqual(validate["package_count"], len(list_scaffold_blueprints()))

            doctor = _run_cli("--pipeline-dir", str(root), "package-doctor")
            self.assertTrue(doctor["ok"])
            self.assertEqual(doctor["readiness"]["warning_count"], 0)
            self.assertTrue(
                all(
                    package["sample_files"]["run_input_example_valid"]
                    for package in doctor["readiness"]["packages"]
                )
            )
            self.assertTrue(
                all(
                    package["sample_files"]["source_list_example_valid"]
                    for package in doctor["readiness"]["packages"]
                )
            )
            self.assertTrue(
                all(
                    package["sample_files"]["source_list_local_sources_present"]
                    for package in doctor["readiness"]["packages"]
                )
            )

    def test_runtime_cli_scaffolds_custom_document_schema_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "custom_package"
            value_schema = root / "document-values.yaml"
            metadata_schema = root / "document-metadata.yaml"
            artifact_schema = root / "export-artifact.yaml"
            output_schema = root / "export-output.yaml"
            stream_contract = root / "ingest-stream.yaml"
            value_schema.write_text(
                textwrap.dedent(
                    """
                    type: object
                    required: ["case_id"]
                    properties:
                      source:
                        type: string
                      case_id:
                        type: string
                      priority:
                        type: integer
                    additionalProperties: true
                    """
                ).strip(),
                encoding="utf-8",
            )
            metadata_schema.write_text(
                textwrap.dedent(
                    """
                    type: object
                    properties:
                      mailbox:
                        type: string
                      received_at:
                        type: string
                    additionalProperties: true
                    """
                ).strip(),
                encoding="utf-8",
            )
            artifact_schema.write_text(
                textwrap.dedent(
                    """
                    type: object
                    required: ["text"]
                    properties:
                      text:
                        type: string
                    additionalProperties: true
                    """
                ).strip(),
                encoding="utf-8",
            )
            output_schema.write_text(
                textwrap.dedent(
                    """
                    type: object
                    required: ["exported_count"]
                    properties:
                      exported_count:
                        type: integer
                    additionalProperties: true
                    """
                ).strip(),
                encoding="utf-8",
            )
            stream_contract.write_text(
                textwrap.dedent(
                    """
                    streams:
                      - stream: records
                        kinds: [record]
                        max_buffered_chunks: 16
                        value_schema:
                          type: object
                          required: ["text"]
                          properties:
                            text:
                              type: string
                    """
                ).strip(),
                encoding="utf-8",
            )

            _run_cli(
                "scaffold",
                "--output-dir",
                str(package_dir),
                "--package-id",
                "custom_package",
                "--pipeline-id",
                "custom_flow",
                "--steps",
                "ingest,export",
                "--document-type",
                "case_document",
                "--document-media-type",
                "application/pdf",
                "--document-extension",
                "pdf",
                "--artifact-extension",
                "ingest=json",
                "--artifact-extension",
                "export=json",
                "--artifact-extension",
                "export=md",
                "--document-value-schema",
                str(value_schema),
                "--document-metadata-schema",
                str(metadata_schema),
                "--artifact-value-schema",
                f"export={artifact_schema}",
                "--capability-output-schema",
                f"export={output_schema}",
                "--stream-contract",
                f"ingest={stream_contract}",
            )

            package_text = (package_dir / "process-runtime-package.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn("id: case_document", package_text)
            self.assertIn('extensions: [".pdf"]', package_text)
            self.assertIn("case_id:", package_text)
            self.assertIn("mailbox:", package_text)
            validate = _run_cli(
                "--pipeline-dir",
                str(root),
                "validate",
                "--json",
            )
            artifact_extensions = {
                item["id"]: item["extensions"]
                for item in validate["packages"][0]["artifact_kinds"]
            }
            artifact_schemas = {
                item["id"]: item["value_schema"]
                for item in validate["packages"][0]["artifact_kinds"]
            }
            capability_schemas = {
                item["id"]: item["output_schema"]
                for item in validate["packages"][0]["capabilities"]
            }
            self.assertEqual(artifact_extensions["ingest_output"], [".json"])
            self.assertEqual(artifact_extensions["export_output"], [".json", ".md"])
            self.assertEqual(
                artifact_schemas["export_output"]["required"],
                ["text"],
            )
            self.assertEqual(
                capability_schemas["export_document"]["required"],
                ["exported_count"],
            )
            self.assertEqual(
                validate["packages"][0]["capabilities"][0]["emits_streams"][0][
                    "stream_id"
                ],
                "records",
            )
            run_input_text = (package_dir / "run-input.example.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn("document_id: sample.pdf", run_input_text)
            self.assertIn("source_uri: file://", run_input_text)
            self.assertIn("/incoming/sample.pdf", run_input_text)
            run_input = _run_cli(
                "--pipeline-dir",
                str(root),
                "validate-run",
                "--run-input",
                str(package_dir / "run-input.example.yaml"),
            )
            self.assertTrue(run_input["ok"])
            self.assertEqual(run_input["documents"][0]["document_type"], "case_document")
            self.assertEqual(
                run_input["documents"][0]["value_keys"],
                ["case_id", "priority", "source"],
            )
            self.assertEqual(
                run_input["documents"][0]["metadata_keys"],
                ["mailbox", "received_at", "sample"],
            )

            source_list_text = (package_dir / "source-list.example.csv").read_text(
                encoding="utf-8"
            )
            self.assertIn("value.case_id", source_list_text)
            self.assertIn("metadata.mailbox", source_list_text)
            source_list_manifest = _run_cli(
                "discover-documents",
                "--source-list",
                str(package_dir / "source-list.example.csv"),
                "--pipeline",
                "custom_flow",
                "--run-id",
                "run_custom_source_list",
            )
            self.assertEqual(
                source_list_manifest["documents"][0]["values"]["priority"],
                1,
            )
            source_manifest_path = root / "custom-source-list-run-input.json"
            source_manifest_path.write_text(
                json.dumps(source_list_manifest),
                encoding="utf-8",
            )
            source_list_preview = _run_cli(
                "--pipeline-dir",
                str(root),
                "validate-run",
                "--run-input",
                str(source_manifest_path),
            )
            self.assertTrue(source_list_preview["ok"])

            db_path = root / "runtime.db"
            _run_cli(
                "--pipeline-dir",
                str(root),
                "create-run",
                "--db",
                str(db_path),
                "--run-input",
                str(package_dir / "run-input.example.yaml"),
            )
            with patch.dict(
                "os.environ",
                {"FALA_ARTIFACT_STORE_ROOT": str(root / "artifact-store")},
            ):
                worked = _run_cli(
                    "--pipeline-dir",
                    str(root),
                    "run-until-idle",
                    "--db",
                    str(db_path),
                    "--pipeline",
                    "custom_flow",
                    "--run-id",
                    "run_custom_flow_sample",
                    "--worker-id",
                    "worker-custom",
                    "--adapter-kind",
                    "subprocess",
                )
            self.assertEqual(worked["completed_count"], 2)
            document = worked["state"]["documents"][0]
            self.assertEqual(document["statuses"]["export"], "completed")
            self.assertEqual(
                document["outputs"]["export"]["values"]["exported_count"],
                0,
            )
            export_artifact = document["outputs"]["export"]["artifacts"][0]
            self.assertEqual(export_artifact["kind"], "export_output")
            export_path = FileArtifactStore(root / "artifact-store").resolve(
                ArtifactRef.model_validate(export_artifact)
            )
            export_payload = json.loads(export_path.read_text())
            self.assertEqual(export_payload["text"], "export sample text")

    def test_runtime_cli_scaffolds_step_policy_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "policy_package"
            review_policy = root / "review-policy.yaml"
            review_policy.write_text(
                textwrap.dedent(
                    """
                    title: Human review
                    adapter:
                      kind: manual
                    priority: 40
                    max_concurrency: 1
                    resource_pool: review_pool
                    resources:
                      memory_mb: 512
                      labels: [human]
                    retry:
                      max_attempts: 3
                      delay_seconds: 5
                      retry_error_kinds: [transient_io]
                      terminal_error_kinds: [validation_error]
                    sla:
                      waiting_after_seconds: 30
                      queued_after_seconds: 60
                      running_after_seconds: 120
                    when:
                      document_types: [case_document]
                      media_types: ["application/pdf"]
                      metadata:
                        stage: review
                    config:
                      form: qa
                    """
                ).strip(),
                encoding="utf-8",
            )

            _run_cli(
                "scaffold",
                "--output-dir",
                str(package_dir),
                "--package-id",
                "policy_package",
                "--pipeline-id",
                "policy_flow",
                "--steps",
                "ingest,review,export",
                "--adapter-kind",
                "queue",
                "--document-type",
                "case_document",
                "--document-media-type",
                "application/pdf",
                "--step-policy",
                f"review={review_policy}",
            )

            pipeline_text = (package_dir / "policy_flow.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn("title: \"Human review\"", pipeline_text)
            self.assertIn("priority: 40", pipeline_text)
            self.assertIn("max_concurrency: 1", pipeline_text)
            self.assertIn("resource_pool: review_pool", pipeline_text)
            self.assertIn("memory_mb: 512", pipeline_text)
            self.assertIn("labels:", pipeline_text)
            self.assertIn("retry_error_kinds:", pipeline_text)
            self.assertIn("terminal_error_kinds:", pipeline_text)
            self.assertIn("waiting_after_seconds: 30", pipeline_text)
            self.assertIn("queued_after_seconds: 60", pipeline_text)
            self.assertIn("running_after_seconds: 120", pipeline_text)
            self.assertIn("document_types:", pipeline_text)
            self.assertIn("form: qa", pipeline_text)
            self.assertIn("kind: manual", pipeline_text)

            package_text = (package_dir / "process-runtime-package.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn("id: ingest_worker", package_text)
            self.assertIn("id: export_worker", package_text)
            self.assertNotIn("id: review_worker", package_text)
            readme_text = (package_dir / "README.scaffold.md").read_text(
                encoding="utf-8"
            )
            self.assertIn("| review | manual | review_document | ingest | 40 | 1 | review_pool |", readme_text)
            self.assertIn("Complete a manual gate", readme_text)
            self.assertIn("--process-id review", readme_text)
            self.assertIn("--package-worker ingest_worker", readme_text)

            validate = _run_cli(
                "--pipeline-dir",
                str(root),
                "validate",
                "--json",
                "--check-commands",
            )
            self.assertTrue(validate["ok"])
            review = next(
                step
                for step in validate["pipelines"][0]["steps"]
                if step["id"] == "review"
            )
            self.assertEqual(review["adapter_kind"], "manual")
            self.assertEqual(review["priority"], 40)
            self.assertEqual(review["max_concurrency"], 1)
            self.assertEqual(review["resource_pool"], "review_pool")
            self.assertEqual(review["sla"]["queued_after_seconds"], 60.0)
            self.assertEqual(
                [worker["id"] for worker in validate["packages"][0]["workers"]],
                ["ingest_worker", "export_worker"],
            )

    def test_runtime_cli_syncs_edited_contract_files_back_to_yaml(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "sync_package"
            _run_cli(
                "scaffold",
                "--output-dir",
                str(package_dir),
                "--package-id",
                "sync_package",
                "--pipeline-id",
                "sync_flow",
                "--steps",
                "ingest,review,export",
                "--adapter-kind",
                "queue",
            )

            policy_dir = package_dir / "contracts" / "policies"
            policy_dir.mkdir(parents=True, exist_ok=True)
            (policy_dir / "review.policy.yaml").write_text(
                textwrap.dedent(
                    """
                    adapter:
                      kind: manual
                    priority: 25
                    config:
                      form: synced-review
                    """
                ).strip(),
                encoding="utf-8",
            )
            (
                package_dir
                / "contracts"
                / "capabilities"
                / "export_document.output.schema.yaml"
            ).write_text(
                textwrap.dedent(
                    """
                    type: object
                    required: ["synced"]
                    properties:
                      synced:
                        type: boolean
                    additionalProperties: true
                    """
                ).strip(),
                encoding="utf-8",
            )

            synced = _run_cli(
                "sync-contracts",
                "--package-yaml",
                str(package_dir / "process-runtime-package.yaml"),
                "--pipeline-yaml",
                str(package_dir / "sync_flow.yaml"),
                "--contract-dir",
                str(package_dir / "contracts"),
            )
            self.assertTrue(synced["ok"])
            self.assertEqual(synced["package_id"], "sync_package")
            self.assertEqual(synced["pipeline_id"], "sync_flow")

            package_text = (package_dir / "process-runtime-package.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn("synced:", package_text)
            self.assertIn("id: ingest_worker", package_text)
            self.assertIn("id: export_worker", package_text)
            self.assertNotIn("id: review_worker", package_text)
            pipeline_text = (package_dir / "sync_flow.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn("kind: manual", pipeline_text)
            self.assertIn("priority: 25", pipeline_text)
            self.assertIn("form: synced-review", pipeline_text)

            validate = _run_cli(
                "--pipeline-dir",
                str(root),
                "validate",
                "--json",
                "--check-commands",
            )
            self.assertTrue(validate["ok"])
            review = next(
                step
                for step in validate["pipelines"][0]["steps"]
                if step["id"] == "review"
            )
            self.assertEqual(review["adapter_kind"], "manual")
            self.assertEqual(review["priority"], 25)
            self.assertEqual(
                [worker["id"] for worker in validate["packages"][0]["workers"]],
                ["ingest_worker", "export_worker"],
            )
            export_capability = next(
                capability
                for capability in validate["packages"][0]["capabilities"]
                if capability["id"] == "export_document"
            )
            self.assertEqual(export_capability["output_schema"]["required"], ["synced"])

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
                "--document-type",
                "email_document",
                "--document-media-type",
                "message/rfc822",
            )
            self.assertEqual(scaffold["adapter_kind"], "queue")
            self.assertEqual(scaffold["document_type"], "email_document")
            self.assertTrue((package_dir / "steps" / "enrich.py").exists())
            readme_text = (package_dir / "README.scaffold.md").read_text(
                encoding="utf-8"
            )
            self.assertIn("--package-worker ingest_worker", readme_text)
            makefile_text = (package_dir / "Makefile").read_text(
                encoding="utf-8"
            )
            self.assertIn("worker:", makefile_text)
            self.assertIn("--package-worker ingest_worker", makefile_text)
            self.assertIn("No subprocess steps declared", makefile_text)
            package_text = (package_dir / "process-runtime-package.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn("document_types:", package_text)
            self.assertIn("id: email_document", package_text)
            self.assertIn('media_types: ["message/rfc822"]', package_text)
            self.assertIn("value_schema:", package_text)
            self.assertIn("source:", package_text)
            self.assertIn("artifact_kinds:", package_text)
            self.assertIn("id: ingest_output", package_text)
            self.assertIn("value_schema:", package_text)
            self.assertIn("capabilities:", package_text)
            self.assertIn("id: ingest_document", package_text)
            self.assertIn("output_schema:", package_text)
            self.assertIn("workers:", package_text)
            self.assertIn("id: ingest_worker", package_text)
            self.assertIn("pipeline: queue_flow", package_text)
            self.assertIn("process: ingest", package_text)
            self.assertIn('capabilities: ["ingest_document"]', package_text)
            self.assertIn('command: ["python", "steps/ingest.py"]', package_text)
            pipeline_text = (package_dir / "queue_flow.yaml").read_text(encoding="utf-8")
            self.assertIn("capability: ingest_document", pipeline_text)
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
                validate["packages"][0]["document_types"][0]["media_types"],
                ["message/rfc822"],
            )
            self.assertEqual(
                validate["packages"][0]["workers"][0]["process_id"],
                "ingest",
            )
            self.assertEqual(
                validate["packages"][0]["workers"][0]["capabilities"],
                ["ingest_document"],
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
                "--capability",
                "ingest_document",
            )
            self.assertEqual(claim["claim"]["process"]["id"], "ingest")
            self.assertEqual(claim["claim"]["process"]["capability"], "ingest_document")
            self.assertEqual(claim["claim"]["process"]["adapter"]["kind"], "queue")
            self.assertEqual(
                claim["claim"]["process"]["adapter"]["queue"],
                "queue_package.ingest",
            )

    def test_runtime_cli_init_project_scaffolds_multi_blueprint_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            project_dir = root / "universal_docs"
            project = _run_cli(
                "init-project",
                "--output-dir",
                str(project_dir),
                "--project-id",
                "universal_docs",
                "--blueprint",
                "document_digitalization",
                "--blueprint",
                "email_processing",
                "--adapter-kind",
                "queue",
            )
            self.assertTrue(project["ok"])
            self.assertEqual(project["project_id"], "universal_docs")
            self.assertEqual(project["adapter_kind"], "queue")
            self.assertEqual(
                project["blueprints"],
                ["document_digitalization", "email_processing"],
            )
            self.assertEqual(project["package_count"], 2)
            self.assertTrue((project_dir / "README.md").is_file())
            self.assertTrue((project_dir / "fala-project.yaml").is_file())
            self.assertTrue((project_dir / "Makefile").is_file())
            self.assertTrue((project_dir / "source-list.example.csv").is_file())
            self.assertTrue((project_dir / "document-routes.example.yaml").is_file())
            self.assertTrue(
                (
                    project_dir
                    / "pipelines"
                    / "document_digitalization"
                    / "process-runtime-package.yaml"
                ).is_file()
            )
            self.assertTrue(
                (
                    project_dir
                    / "pipelines"
                    / "email_processing"
                    / "Makefile"
                ).is_file()
            )
            readme_text = (project_dir / "README.md").read_text(encoding="utf-8")
            self.assertIn("pipelines/document_digitalization", readme_text)
            self.assertIn("pipelines/email_processing", readme_text)
            self.assertIn("make bootstrap", readme_text)
            self.assertIn("make project-doctor", readme_text)
            self.assertIn("make project-spec", readme_text)
            self.assertIn("make project-smoke", readme_text)
            project_yaml_text = (project_dir / "fala-project.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn("project: universal_docs", project_yaml_text)
            self.assertIn("pipeline_dir: pipelines", project_yaml_text)
            self.assertIn("routes: document-routes.example.yaml", project_yaml_text)
            self.assertIn("document_type: generic_document", project_yaml_text)
            self.assertIn("alerts:", project_yaml_text)
            self.assertIn("metric: queue.worker_deficit_count", project_yaml_text)
            self.assertIn("metric: supervision.dead_letter_count", project_yaml_text)
            self.assertIn("lifecycle:", project_yaml_text)
            self.assertIn("older_than_days: 30", project_yaml_text)
            self.assertIn("- completed", project_yaml_text)
            makefile_text = (project_dir / "Makefile").read_text(encoding="utf-8")
            self.assertIn("FALA ?= fala", makefile_text)
            self.assertIn("PROJECT_DOCTOR ?= project-doctor.json", makefile_text)
            self.assertIn("PROJECT_CHECK ?= project-check.json", makefile_text)
            self.assertIn("PROJECT_SMOKE ?= project-smoke.json", makefile_text)
            self.assertIn("DB_DOCTOR ?= db-doctor.json", makefile_text)
            self.assertIn("PROJECT_SPEC ?= project-spec.json", makefile_text)
            self.assertIn("PROJECT_SECRETS ?= project-secrets.json", makefile_text)
            self.assertIn("ENV_EXAMPLE ?= .env.example", makefile_text)
            self.assertIn("PROJECT_BUNDLE ?= fala-project-bundle.tar.gz", makefile_text)
            self.assertIn("PROJECT_OPERATIONS ?= project-operations.json", makefile_text)
            self.assertIn("PROJECT_ALERTS ?= project-alerts.json", makefile_text)
            self.assertIn("PROJECT_LIFECYCLE ?= project-lifecycle.json", makefile_text)
            self.assertIn(
                "PACKAGE_DIRS := pipelines/document_digitalization pipelines/email_processing",
                makefile_text,
            )
            self.assertIn("project-doctor:", makefile_text)
            self.assertIn("project-check:", makefile_text)
            self.assertIn("project-smoke:", makefile_text)
            self.assertIn("db-doctor:", makefile_text)
            self.assertIn("project-spec:", makefile_text)
            self.assertIn("project-secrets:", makefile_text)
            self.assertIn("project-bundle:", makefile_text)
            self.assertIn("project-bundle-verify:", makefile_text)
            self.assertIn("project-supervision:", makefile_text)
            self.assertIn("project-operations:", makefile_text)
            self.assertIn("project-alerts:", makefile_text)
            self.assertIn("project-lifecycle:", makefile_text)
            self.assertIn("--project-dir .", makefile_text)
            self.assertIn("mixed-source-list:", makefile_text)
            self.assertIn("ROUTES ?= document-routes.example.yaml", makefile_text)
            self.assertIn("--route $(ROUTES)", makefile_text)
            self.assertIn("--auto-route", makefile_text)
            self.assertIn("create-mixed:", makefile_text)
            self.assertIn("create-project-run", makefile_text)
            self.assertIn("package-index:", makefile_text)
            self.assertIn("worker-commands:", makefile_text)
            self.assertIn("deployment-compose:", makefile_text)
            self.assertIn("--container-pipeline-dir $(CONTAINER_PIPELINE_DIR)", makefile_text)
            self.assertIn("package-bootstrap:", makefile_text)
            self.assertIn("serve:", makefile_text)
            source_list_text = (project_dir / "source-list.example.csv").read_text(
                encoding="utf-8"
            )
            self.assertIn("document_digitalization_sample.pdf", source_list_text)
            self.assertIn("document_digitalization_page_sample.pdf", source_list_text)
            self.assertIn("parent_document_id", source_list_text)
            self.assertIn("parent_process_id", source_list_text)
            self.assertIn("email_processing_sample.eml", source_list_text)
            self.assertIn("email_processing_attachment_sample.bin", source_list_text)
            routes_text = (project_dir / "document-routes.example.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn("id: document_digitalization", routes_text)
            self.assertIn("pipeline_id: document_digitalization_flow", routes_text)
            self.assertIn("id: document_digitalization_page_document", routes_text)
            self.assertIn("document_type: page_document", routes_text)
            self.assertIn("id: email_processing_email_attachment_document", routes_text)
            self.assertIn("document_type: email_attachment_document", routes_text)
            self.assertIn("document_types:", routes_text)

            validate = _run_cli(
                "--pipeline-dir",
                str(project_dir / "pipelines"),
                "validate",
                "--json",
                "--check-commands",
            )
            self.assertTrue(validate["ok"])
            self.assertEqual(validate["package_count"], 2)
            doctor = _run_cli(
                "--pipeline-dir",
                str(project_dir / "pipelines"),
                "package-doctor",
            )
            self.assertTrue(doctor["ok"])
            self.assertEqual(doctor["readiness"]["package_count"], 2)
            self.assertTrue(
                all(
                    package["sample_files"]["makefile"]
                    for package in doctor["readiness"]["packages"]
                )
            )
            project_doctor = _run_cli(
                "project-doctor",
                "--project-dir",
                str(project_dir),
            )
            self.assertTrue(project_doctor["ok"])
            self.assertEqual(project_doctor["project_id"], "universal_docs")
            self.assertEqual(project_doctor["readiness"]["package_count"], 2)
            self.assertEqual(project_doctor["alerts"]["rule_count"], 4)
            self.assertTrue(project_doctor["lifecycle"]["run_retention"]["enabled"])
            self.assertEqual(
                project_doctor["lifecycle"]["run_retention"]["older_than_days"],
                30.0,
            )
            self.assertEqual(project_doctor["mixed_run_input"]["document_count"], 4)
            self.assertEqual(
                project_doctor["mixed_run_input"]["pipeline_counts"],
                {
                    "document_digitalization_flow": 2,
                    "email_processing_flow": 2,
                },
            )
            self.assertTrue(project_doctor["sample_files"]["project_yaml"])
            project_doctor_path = project_dir / "project-doctor.json"
            project_doctor_file = _run_cli(
                "project-doctor",
                "--project-dir",
                str(project_dir),
                "--output",
                str(project_doctor_path),
            )
            self.assertTrue(project_doctor_file["ok"])
            self.assertEqual(str(project_doctor_path), project_doctor_file["output"])
            self.assertTrue(project_doctor_path.is_file())
            project_spec = _run_cli(
                "project-spec",
                "--project-dir",
                str(project_dir),
                "--base-url",
                "http://localhost:8000",
                "--run-id",
                "run_spec",
            )
            self.assertTrue(project_spec["ok"])
            self.assertEqual(project_spec["project_id"], "universal_docs")
            self.assertEqual(project_spec["package_index"]["package_count"], 2)
            self.assertEqual(project_spec["intake"]["document_count"], 4)
            self.assertEqual(project_spec["routes"]["count"], 4)
            self.assertGreater(project_spec["worker_commands"]["worker_count"], 0)
            self.assertEqual(project_spec["worker_commands"]["run_id"], "run_spec")
            self.assertEqual(project_spec["alerts"]["rule_count"], 4)
            self.assertTrue(project_spec["lifecycle"]["run_retention"]["enabled"])
            project_spec_path = project_dir / "project-spec.json"
            project_spec_file = _run_cli(
                "project-spec",
                "--project-dir",
                str(project_dir),
                "--output",
                str(project_spec_path),
            )
            self.assertTrue(project_spec_file["ok"])
            self.assertEqual(str(project_spec_path), project_spec_file["output"])
            self.assertTrue(project_spec_path.is_file())
            project_run = _run_cli(
                "create-project-run",
                "--project-dir",
                str(project_dir),
                "--db",
                str(project_dir / "runtime.db"),
                "--run-id",
                "run_project_cli",
                "--metadata",
                "operator=cli",
            )
            self.assertTrue(project_run["ok"])
            self.assertEqual(project_run["document_count"], 4)
            self.assertEqual(project_run["route_report"]["routed_count"], 4)
            stored_project_run = asyncio.run(
                SQLiteStateStore(project_dir / "runtime.db").get_run("run_project_cli")
            )
            self.assertIsNotNone(stored_project_run)
            assert stored_project_run is not None
            self.assertEqual(
                stored_project_run.metadata["project_id"],
                "universal_docs",
            )
            self.assertEqual(stored_project_run.metadata["operator"], "cli")
            self.assertEqual(
                stored_project_run.metadata["process_runtime"]["project"]["project_id"],
                "universal_docs",
            )
            missing_db_code, missing_db = _run_cli_raw(
                "db-doctor",
                "--db",
                str(project_dir / "missing.db"),
            )
            self.assertEqual(missing_db_code, 1)
            self.assertFalse(missing_db["ok"])
            self.assertEqual(missing_db["store_kind"], "sqlite")
            self.assertIn("does not exist", missing_db["error"])
            db_doctor = _run_cli(
                "db-doctor",
                "--db",
                str(project_dir / "runtime.db"),
            )
            self.assertTrue(db_doctor["ok"])
            self.assertEqual(db_doctor["store_kind"], "sqlite")
            self.assertEqual(db_doctor["counts"]["runs"], 1)
            self.assertEqual(db_doctor["counts"]["documents"], 4)
            self.assertGreaterEqual(db_doctor["counts"]["processes"], 2)
            self.assertEqual(db_doctor["schema"]["missing_tables"], [])
            self.assertEqual(db_doctor["schema"]["current_version"], RUNTIME_SCHEMA_VERSION)
            self.assertEqual(db_doctor["schema"]["latest_version"], RUNTIME_SCHEMA_VERSION)
            self.assertTrue(db_doctor["schema"]["migrations"]["ok"])
            self.assertEqual(db_doctor["schema"]["migrations"]["missing"], [])
            db_doctor_path = project_dir / "db-doctor.json"
            db_doctor_file = _run_cli(
                "db-doctor",
                "--db",
                str(project_dir / "runtime.db"),
                "--ensure-schema",
                "--output",
                str(db_doctor_path),
            )
            self.assertTrue(db_doctor_file["ok"])
            self.assertEqual(str(db_doctor_path), db_doctor_file["output"])
            self.assertEqual(db_doctor_file["current_version"], RUNTIME_SCHEMA_VERSION)
            self.assertEqual(db_doctor_file["missing_migration_count"], 0)
            self.assertTrue(db_doctor_path.is_file())
            project_supervision = _run_cli(
                "project-supervision",
                "--project-dir",
                str(project_dir),
                "--db",
                str(project_dir / "runtime.db"),
                "--queued-after-seconds",
                "0",
            )
            self.assertTrue(project_supervision["ok"])
            self.assertEqual(project_supervision["project_id"], "universal_docs")
            self.assertEqual(project_supervision["supervision"]["run_count"], 1)
            self.assertGreaterEqual(
                project_supervision["supervision"]["stuck_work_count"],
                1,
            )
            project_operations = _run_cli(
                "project-operations",
                "--project-dir",
                str(project_dir),
                "--db",
                str(project_dir / "runtime.db"),
                "--queued-after-seconds",
                "0",
            )
            self.assertTrue(project_operations["ok"])
            self.assertEqual(project_operations["operations"]["status"], "critical")
            self.assertGreaterEqual(
                project_operations["operations"]["queue"]["worker_deficit_count"],
                1,
            )
            project_alerts = _run_cli(
                "project-alerts",
                "--project-dir",
                str(project_dir),
                "--db",
                str(project_dir / "runtime.db"),
                "--queued-after-seconds",
                "0",
            )
            self.assertTrue(project_alerts["ok"])
            self.assertEqual(project_alerts["alerts"]["status"], "critical")
            self.assertIn(
                "worker_deficit_present",
                {item["rule_id"] for item in project_alerts["alerts"]["alerts"]},
            )
            lifecycle_path = project_dir / "project-lifecycle.json"
            project_lifecycle = _run_cli(
                "project-lifecycle",
                "--project-dir",
                str(project_dir),
                "--db",
                str(project_dir / "runtime.db"),
                "--before",
                "2999-01-01T00:00:00+00:00",
                "--status",
                "queued",
                "--skip-artifact-gc",
                "--output",
                str(lifecycle_path),
            )
            self.assertTrue(project_lifecycle["ok"])
            self.assertEqual(str(lifecycle_path), project_lifecycle["output"])
            self.assertTrue(lifecycle_path.is_file())
            project_lifecycle_payload = json.loads(
                lifecycle_path.read_text(encoding="utf-8")
            )
            self.assertEqual(
                project_lifecycle_payload["lifecycle"]["candidate_count"],
                1,
            )
            self.assertTrue(project_lifecycle_payload["lifecycle"]["dry_run"])
            (project_dir / ".env").write_text(
                "OPENAI_API_KEY=real-secret\n",
                encoding="utf-8",
            )
            bundle_path = project_dir / "universal-docs.tar.gz"
            project_bundle = _run_cli(
                "project-bundle",
                "--project-dir",
                str(project_dir),
                "--output",
                str(bundle_path),
                "--base-url",
                "http://localhost:8000",
                "--run-id",
                "run_bundle",
            )
            self.assertTrue(project_bundle["ok"])
            self.assertEqual(str(bundle_path.resolve()), project_bundle["output"])
            self.assertEqual(project_bundle["bundle_name"], "universal_docs")
            self.assertGreater(project_bundle["file_count"], 0)
            self.assertGreaterEqual(project_bundle["generated_file_count"], 5)
            self.assertTrue(bundle_path.is_file())
            with tarfile.open(bundle_path, "r:gz") as archive:
                names = set(archive.getnames())
                self.assertIn("universal_docs/fala-project.yaml", names)
                self.assertIn("universal_docs/project-spec.json", names)
                self.assertIn("universal_docs/project-secrets.json", names)
                self.assertIn("universal_docs/package-index.json", names)
                self.assertIn("universal_docs/.env.example", names)
                self.assertIn("universal_docs/bundle-manifest.json", names)
                self.assertIn(
                    (
                        "universal_docs/pipelines/document_digitalization/"
                        "process-runtime-package.yaml"
                    ),
                    names,
                )
                self.assertNotIn("universal_docs/.env", names)
                self.assertNotIn("universal_docs/runtime.db", names)
                manifest_member = archive.extractfile(
                    "universal_docs/bundle-manifest.json"
                )
                self.assertIsNotNone(manifest_member)
                assert manifest_member is not None
                bundle_manifest = json.loads(
                    manifest_member.read().decode("utf-8")
                )
                self.assertEqual(bundle_manifest["project_id"], "universal_docs")
                self.assertEqual(bundle_manifest["run_id"], "run_bundle")
                self.assertTrue(
                    any(
                        item["path"] == "project-spec.json"
                        for item in bundle_manifest["generated_files"]
                    )
                )
            verified_bundle = _run_cli(
                "project-bundle-verify",
                str(bundle_path),
            )
            self.assertTrue(verified_bundle["ok"])
            self.assertEqual(verified_bundle["bundle_name"], "universal_docs")
            self.assertEqual(verified_bundle["project_id"], "universal_docs")
            self.assertGreater(verified_bundle["checked_file_count"], 0)
            project_check_path = project_dir / "project-check.json"
            project_check = _run_cli(
                "project-check",
                "--project-dir",
                str(project_dir),
                "--db",
                str(project_dir / "runtime.db"),
                "--ensure-schema",
                "--bundle",
                str(bundle_path),
                "--run-id",
                "run_check",
                "--output",
                str(project_check_path),
            )
            self.assertTrue(project_check["ok"])
            self.assertEqual(str(project_check_path), project_check["output"])
            self.assertEqual(project_check["check_count"], 5)
            self.assertEqual(project_check["failed_check_count"], 0)
            project_check_payload = json.loads(
                project_check_path.read_text(encoding="utf-8")
            )
            self.assertTrue(project_check_payload["ok"])
            self.assertEqual(
                {item["id"] for item in project_check_payload["checks"]},
                {
                    "project_readiness",
                    "project_spec",
                    "project_secrets",
                    "runtime_database",
                    "project_bundle",
                },
            )
            self.assertEqual(project_check_payload["db"]["counts"]["runs"], 1)
            self.assertTrue(project_check_payload["bundle"]["ok"])
            runtime_database_check = next(
                item
                for item in project_check_payload["checks"]
                if item["id"] == "runtime_database"
            )
            self.assertEqual(
                runtime_database_check["summary"]["current_version"],
                RUNTIME_SCHEMA_VERSION,
            )
            self.assertEqual(
                runtime_database_check["summary"]["missing_migration_count"],
                0,
            )
            project_smoke_path = project_dir / "project-smoke.json"
            project_smoke = _run_cli(
                "project-smoke",
                "--project-dir",
                str(project_dir),
                "--db",
                str(project_dir / "runtime.db"),
                "--run-id",
                "run_project_smoke",
                "--output",
                str(project_smoke_path),
            )
            self.assertTrue(project_smoke["ok"])
            self.assertEqual(str(project_smoke_path), project_smoke["output"])
            self.assertEqual(project_smoke["run_status"], "completed")
            self.assertGreater(project_smoke["completed_count"], 0)
            project_smoke_payload = json.loads(
                project_smoke_path.read_text(encoding="utf-8")
            )
            self.assertTrue(project_smoke_payload["ok"])
            self.assertEqual(project_smoke_payload["failed_count"], 0)
            self.assertEqual(project_smoke_payload["run_status"], "completed")
            self.assertEqual(project_smoke_payload["state_summary"]["document_count"], 4)
            self.assertGreater(project_smoke_payload["queue_worker_count"], 0)
            self.assertEqual(
                {
                    step["adapter_kind"]
                    for step in project_smoke_payload["steps"]
                },
                {"queue"},
            )

            tampered_path = project_dir / "tampered.tar.gz"

            def add_text(
                archive: tarfile.TarFile,
                name: str,
                text: str,
            ) -> None:
                data = text.encode("utf-8")
                info = tarfile.TarInfo(name)
                info.size = len(data)
                archive.addfile(info, BytesIO(data))

            tampered_manifest = {
                "bundle_name": "bad",
                "project_id": "bad",
                "files": [],
                "generated_files": [],
            }
            with tarfile.open(tampered_path, "w:gz") as archive:
                add_text(
                    archive,
                    "bad/bundle-manifest.json",
                    json.dumps(tampered_manifest),
                )
                for name in [
                    ".env.example",
                    "fala-project.yaml",
                    "package-index.json",
                    "project-secrets.json",
                    "project-spec.json",
                ]:
                    add_text(archive, f"bad/{name}", "{}")
                add_text(archive, "bad/.env", "OPENAI_API_KEY=real-secret")
            code, tampered = _run_cli_raw(
                "project-bundle-verify",
                str(tampered_path),
            )
            self.assertEqual(code, 1)
            self.assertFalse(tampered["ok"])
            self.assertIn(
                "bundle_runtime_state_included",
                {item["code"] for item in tampered["errors"]},
            )
            routed = _run_cli(
                "--pipeline-dir",
                str(project_dir / "pipelines"),
                "discover-documents",
                "--source-list",
                str(project_dir / "source-list.example.csv"),
                "--route",
                str(project_dir / "document-routes.example.yaml"),
                "--run-id",
                "run_routed",
            )
            self.assertEqual(
                [document["pipeline_id"] for document in routed["documents"]],
                [
                    "document_digitalization_flow",
                    "document_digitalization_flow",
                    "email_processing_flow",
                    "email_processing_flow",
                ],
            )
            self.assertEqual(
                routed["documents"][0]["metadata"]["blueprint"],
                "document_digitalization",
            )
            mixed = _run_cli(
                "--pipeline-dir",
                str(project_dir / "pipelines"),
                "discover-documents",
                "--source-list",
                str(project_dir / "source-list.example.csv"),
                "--route",
                str(project_dir / "document-routes.example.yaml"),
                "--auto-route",
                "--run-id",
                "run_mixed",
            )
            self.assertNotIn("pipeline_id", mixed)
            self.assertEqual(
                {
                    document["document_id"]: (
                        document["pipeline_id"],
                        document["document_type"],
                    )
                    for document in mixed["documents"]
                },
                {
                    "document_digitalization_sample.pdf": (
                        "document_digitalization_flow",
                        "generic_document",
                    ),
                    "document_digitalization_page_sample.pdf": (
                        "document_digitalization_flow",
                        "page_document",
                    ),
                    "email_processing_sample.eml": (
                        "email_processing_flow",
                        "email_document",
                    ),
                    "email_processing_attachment_sample.bin": (
                        "email_processing_flow",
                        "email_attachment_document",
                    ),
                },
            )
            mixed_path = project_dir / "run-input.mixed.json"
            mixed_path.write_text(json.dumps(mixed), encoding="utf-8")
            mixed_preview = _run_cli(
                "--pipeline-dir",
                str(project_dir / "pipelines"),
                "validate-run",
                "--run-input",
                str(mixed_path),
            )
            self.assertTrue(mixed_preview["ok"])
            self.assertEqual(mixed_preview["document_count"], 4)
            index_path = project_dir / "package-index.json"
            index = _run_cli(
                "--pipeline-dir",
                str(project_dir / "pipelines"),
                "package-index",
                "--output",
                str(index_path),
            )
            self.assertTrue(index["ok"])
            self.assertTrue(index_path.is_file())
            self.assertEqual(index["package_count"], 2)
            commands = _run_cli(
                "--pipeline-dir",
                str(project_dir / "pipelines"),
                "worker-commands",
                "--base-url",
                "http://localhost:8000",
                "--run-id",
                "run_mixed",
            )
            self.assertEqual(commands["run_id"], "run_mixed")
            self.assertGreater(len(commands["workers"]), 0)
            deployment = _run_cli(
                "--pipeline-dir",
                str(project_dir / "pipelines"),
                "deployment",
                "--format",
                "docker-compose",
                "--run-id",
                "run_mixed",
                "--image",
                "fala:test",
                "--worker-image",
                "fala-worker:test",
            )
            self.assertTrue(deployment["ok"])
            self.assertEqual(deployment["worker_count"], 11)
            self.assertIn("fala-control-plane", deployment["manifest"])

    def test_runtime_cli_init_project_accepts_custom_blueprint_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            blueprint_file = root / "invoice-blueprint.yaml"
            project_dir = root / "invoice_workspace"
            blueprint_file.write_text(
                textwrap.dedent(
                    """
                    id: invoice_review
                    title: Invoice review
                    document:
                      type: invoice_document
                      media_types: [application/pdf]
                      extensions: [.pdf]
                      value_schema:
                        type: object
                        properties:
                          source:
                            type: string
                          vendor:
                            type: string
                        additionalProperties: true
                    steps:
                      - id: ingest
                        capability: ingest_invoice
                        artifact_kind: source_invoice
                        artifact_media_types: [application/json]
                      - id: extract
                        capability: extract_invoice
                        artifact_kind: extracted_invoice
                        artifact_media_types: [application/json]
                        streams:
                          - stream_id: pages
                            kinds: [page]
                            value_schema:
                              type: object
                              properties:
                                text:
                                  type: string
                              additionalProperties: true
                    """
                ).strip(),
                encoding="utf-8",
            )

            project = _run_cli(
                "init-project",
                "--output-dir",
                str(project_dir),
                "--project-id",
                "invoice_workspace",
                "--blueprint-file",
                str(blueprint_file),
                "--adapter-kind",
                "queue",
            )
            self.assertTrue(project["ok"])
            self.assertEqual(project["blueprints"], ["invoice_review"])
            self.assertEqual(project["package_count"], 1)
            self.assertTrue(
                (
                    project_dir
                    / "pipelines"
                    / "invoice_review"
                    / "process-runtime-package.yaml"
                ).is_file()
            )

            project_yaml_text = (project_dir / "fala-project.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn("blueprint: invoice_review", project_yaml_text)
            self.assertIn("blueprint_source:", project_yaml_text)
            self.assertIn(str(blueprint_file.resolve()), project_yaml_text)
            readme_text = (project_dir / "README.md").read_text(encoding="utf-8")
            self.assertIn("Invoice review", readme_text)
            source_list_text = (project_dir / "source-list.example.csv").read_text(
                encoding="utf-8"
            )
            self.assertIn("invoice_review_sample.pdf", source_list_text)
            routes_text = (project_dir / "document-routes.example.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn("pipeline_id: invoice_review_flow", routes_text)
            self.assertIn("document_types:", routes_text)

            validate = _run_cli(
                "--pipeline-dir",
                str(project_dir / "pipelines"),
                "validate",
                "--json",
                "--check-commands",
            )
            self.assertTrue(validate["ok"])
            self.assertEqual(validate["package_count"], 1)
            self.assertEqual(
                validate["packages"][0]["document_types"][0]["id"],
                "invoice_document",
            )
            project_doctor = _run_cli(
                "project-doctor",
                "--project-dir",
                str(project_dir),
            )
            self.assertTrue(project_doctor["ok"])
            self.assertEqual(project_doctor["mixed_run_input"]["document_count"], 1)
            self.assertEqual(
                project_doctor["mixed_run_input"]["pipeline_counts"],
                {"invoice_review_flow": 1},
            )
            project_spec = _run_cli(
                "project-spec",
                "--project-dir",
                str(project_dir),
            )
            self.assertTrue(project_spec["ok"])
            self.assertEqual(project_spec["package_index"]["package_count"], 1)
            self.assertEqual(project_spec["intake"]["document_count"], 1)

    def test_runtime_cli_project_secrets_exports_inventory_and_env_template(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            project_dir = Path(tmp) / "secret_workspace"
            package_dir = project_dir / "pipelines" / "secret_pkg"
            package_dir.mkdir(parents=True)
            (project_dir / "fala-project.yaml").write_text(
                textwrap.dedent(
                    """
                    project: secret_workspace
                    pipeline_dir: pipelines
                    packages:
                      - id: secret_pkg
                        package_dir: pipelines/secret_pkg
                        package_id: secret_pkg
                        pipeline_id: extract_flow
                        document_type: generic_document
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: secret_pkg
                    document_types:
                      - id: generic_document
                    operation_types:
                      - id: extract
                        category: extraction
                    artifact_kinds:
                      - id: extracted_payload
                    capabilities:
                      - id: extract_text
                        accepts_document_types: [generic_document]
                        emits_artifact_kinds: [extracted_payload]
                    secrets:
                      - id: openai_api_key
                        env_var: OPENAI_API_KEY
                        description: OpenAI API key
                        kubernetes_secret_name: fala-openai
                        kubernetes_secret_key: api-key
                      - id: optional_proxy
                        env_var: HTTP_PROXY
                        required: false
                    pipelines:
                      - extract.yaml
                    workers:
                      - id: extract_worker
                        pipeline: extract_flow
                        process: extract
                        capabilities: [extract_text]
                        command: ["python", "-c", "print('ok')"]
                        secrets: [openai_api_key, optional_proxy]
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "extract.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: extract_flow
                    steps:
                      - id: extract
                        capability: extract_text
                        adapter:
                          kind: queue
                          queue: secret.extract
                    """
                ).strip(),
                encoding="utf-8",
            )

            output_path = project_dir / "project-secrets.json"
            env_path = project_dir / ".env.example"
            exported = _run_cli(
                "project-secrets",
                "--project-dir",
                str(project_dir),
                "--output",
                str(output_path),
                "--env-output",
                str(env_path),
            )
            self.assertTrue(exported["ok"])
            self.assertEqual(exported["secret_count"], 2)
            self.assertEqual(exported["required_count"], 1)
            self.assertEqual(str(output_path), exported["output"])
            self.assertEqual(str(env_path), exported["env_output"])
            self.assertTrue(output_path.is_file())
            self.assertTrue(env_path.is_file())
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["project_id"], "secret_workspace")
            self.assertEqual(payload["secrets"]["env_var_count"], 2)
            self.assertEqual(
                payload["secrets"]["env_vars"][0]["env_var"],
                "OPENAI_API_KEY",
            )
            self.assertEqual(
                payload["secrets"]["env_vars"][0]["kubernetes_refs"],
                ["fala-openai:api-key"],
            )
            self.assertEqual(
                payload["secrets"]["packages"][0]["secrets"][0]["worker_ids"],
                ["extract_worker"],
            )
            env_text = env_path.read_text(encoding="utf-8")
            self.assertIn("OPENAI_API_KEY=", env_text)
            self.assertIn("HTTP_PROXY=", env_text)
            self.assertIn("# FALA_API_KEYS=", env_text)

            inline = _run_cli(
                "project-secrets",
                "--project-dir",
                str(project_dir),
                "--no-auth-placeholders",
            )
            self.assertTrue(inline["ok"])
            self.assertEqual(inline["secrets"]["secret_count"], 2)
            self.assertNotIn("FALA_API_KEYS", inline["env_template"])

            spec = _run_cli(
                "project-spec",
                "--project-dir",
                str(project_dir),
            )
            self.assertEqual(spec["secrets"]["secret_count"], 2)
            self.assertEqual(spec["secrets"]["required_count"], 1)

    def test_runtime_cli_lists_scaffold_blueprint_catalog(self) -> None:
        catalog = _run_cli("scaffold-blueprints")
        self.assertEqual(catalog["blueprint_count"], 10)
        translated_query = _run_cli(
            "scaffold-blueprints",
            "--query",
            "translated_document",
        )
        self.assertEqual(translated_query["query"], "translated_document")
        self.assertEqual(
            [item["id"] for item in translated_query["blueprints"]],
            ["document_translation_review"],
        )
        package_query = _run_cli(
            "scaffold-blueprints",
            "--query",
            "package item",
        )
        self.assertEqual(
            [item["id"] for item in package_query["blueprints"]],
            ["document_package_processing"],
        )
        self.assertEqual(
            [item["id"] for item in catalog["blueprints"]],
            [
                "document_digitalization",
                "email_processing",
                "document_package_processing",
                "document_redaction_review",
                "document_translation_review",
                "generative_media",
                "llm_document_processing",
                "knowledge_base_ingestion",
                "structured_extraction_review",
                "tabular_data_processing",
            ],
        )
        generative = next(
            item for item in catalog["blueprints"] if item["id"] == "generative_media"
        )
        self.assertEqual(generative["document"]["type"], "creative_brief")
        self.assertEqual(generative["step_count"], 5)
        self.assertEqual(generative["stream_count"], 3)
        self.assertEqual(
            [operation["id"] for operation in generative["operation_types"]],
            ["ingest", "plan", "generate", "render", "export"],
        )
        self.assertIn("accelerator_pool", generative["resource_pools"])
        self.assertIn("--blueprint generative_media", generative["scaffold_command"])
        digitalization = next(
            item
            for item in catalog["blueprints"]
            if item["id"] == "document_digitalization"
        )
        self.assertEqual(
            digitalization["additional_document_types"][0]["id"],
            "page_document",
        )
        self.assertEqual(
            digitalization["additional_document_relations"][0]["id"],
            "page",
        )
        digitalization_steps = {
            step["id"]: step for step in digitalization["steps"]
        }
        self.assertEqual(
            digitalization_steps["extract"]["guidance"]["operation_type"],
            "extract",
        )
        self.assertIn(
            "artifact_kind",
            digitalization_steps["extract"]["guidance"]["outputs"],
        )
        self.assertEqual(
            digitalization_steps["ingest"]["accepts_document_types"],
            ["generic_document", "page_document"],
        )
        self.assertEqual(digitalization_steps["extract"]["needs"], ["ingest"])
        self.assertEqual(digitalization_steps["normalize"]["needs"], ["ingest"])
        self.assertEqual(
            digitalization_steps["extract"]["accepts_document_types"],
            ["generic_document"],
        )
        self.assertEqual(digitalization_steps["extract"]["operation_type"], "extract")
        self.assertEqual(
            digitalization_steps["normalize"]["accepts_document_types"],
            ["page_document"],
        )
        self.assertEqual(
            digitalization_steps["extract"]["emits_document_types"],
            ["page_document"],
        )
        self.assertEqual(digitalization_steps["assemble"]["needs"], ["extract"])
        self.assertEqual(
            digitalization_steps["assemble"]["accepts_document_types"],
            ["generic_document"],
        )
        self.assertEqual(
            digitalization_steps["assemble"]["policy"]["when"]["document_types"],
            ["generic_document"],
        )
        self.assertEqual(
            digitalization_steps["assemble"]["policy"]["wait_for_children"],
            {
                "from_processes": ["extract"],
                "document_types": ["page_document"],
                "relations": ["page"],
                "min_count": 1,
            },
        )
        self.assertEqual(
            digitalization_steps["extract"]["policy"]["when"]["document_types"],
            ["generic_document"],
        )
        self.assertEqual(
            digitalization_steps["normalize"]["policy"]["when"]["document_types"],
            ["page_document"],
        )
        email = next(
            item for item in catalog["blueprints"] if item["id"] == "email_processing"
        )
        self.assertEqual(
            email["additional_document_types"][0]["id"],
            "email_attachment_document",
        )
        self.assertEqual(
            email["additional_document_relations"][0]["id"],
            "attachment",
        )
        email_steps = {step["id"]: step for step in email["steps"]}
        self.assertEqual(
            email_steps["ingest_email"]["accepts_document_types"],
            ["email_document", "email_attachment_document"],
        )
        self.assertEqual(email_steps["classify"]["needs"], ["ingest_email"])
        self.assertEqual(
            email_steps["extract_attachments"]["emits_document_types"],
            ["email_attachment_document"],
        )
        self.assertEqual(
            email_steps["extract_attachments"]["operation_type"],
            "extract",
        )
        self.assertEqual(
            email_steps["parse_message"]["policy"]["when"]["document_types"],
            ["email_document"],
        )

        package = next(
            item
            for item in catalog["blueprints"]
            if item["id"] == "document_package_processing"
        )
        self.assertEqual(package["document"]["type"], "document_package")
        self.assertEqual(
            package["additional_document_types"][0]["id"],
            "packaged_document",
        )
        self.assertEqual(
            package["additional_document_relations"][0]["id"],
            "package_item",
        )
        self.assertEqual(package["step_count"], 7)
        self.assertEqual(package["stream_count"], 2)
        self.assertIn("io_pool", package["resource_pools"])
        self.assertIn("llm_pool", package["resource_pools"])
        package_steps = {step["id"]: step for step in package["steps"]}
        self.assertEqual(
            package_steps["ingest_package"]["accepts_document_types"],
            ["document_package", "packaged_document"],
        )
        self.assertEqual(
            package_steps["inspect_package"]["operation_type"],
            "analyze",
        )
        self.assertEqual(
            package_steps["inspect_package"]["streams"][0]["stream_id"],
            "manifest_items",
        )
        self.assertEqual(
            package_steps["extract_items"]["emits_document_types"],
            ["packaged_document"],
        )
        self.assertEqual(
            package_steps["classify_item"]["policy"]["when"]["document_types"],
            ["packaged_document"],
        )
        self.assertEqual(
            package_steps["export_manifest"]["policy"]["wait_for_children"],
            {
                "from_processes": ["extract_items"],
                "document_types": ["packaged_document"],
                "relations": ["package_item"],
                "min_count": 1,
            },
        )

        redaction = next(
            item
            for item in catalog["blueprints"]
            if item["id"] == "document_redaction_review"
        )
        self.assertEqual(redaction["document"]["type"], "sensitive_document")
        self.assertEqual(
            redaction["additional_document_types"][0]["id"],
            "redacted_document",
        )
        self.assertEqual(
            redaction["additional_document_relations"][0]["id"],
            "redacted",
        )
        self.assertEqual(redaction["manual_steps"], ["review_redaction"])
        self.assertEqual(redaction["stream_count"], 3)
        self.assertIn("pii_pool", redaction["resource_pools"])
        self.assertIn("redaction_pool", redaction["resource_pools"])
        redaction_steps = {step["id"]: step for step in redaction["steps"]}
        self.assertEqual(
            redaction_steps["detect_sensitive_data"]["operation_type"],
            "detect",
        )
        self.assertEqual(
            redaction_steps["ingest"]["accepts_document_types"],
            ["sensitive_document", "redacted_document"],
        )
        self.assertEqual(
            redaction_steps["redact_document"]["operation_type"],
            "redact",
        )
        self.assertEqual(
            redaction_steps["review_redaction"]["policy"]["adapter_kind"],
            "manual",
        )
        self.assertEqual(
            redaction_steps["redact_document"]["emits_document_types"],
            ["redacted_document"],
        )
        self.assertEqual(
            redaction_steps["detect_sensitive_data"]["streams"][0]["stream_id"],
            "sensitive_spans",
        )

        translation = next(
            item
            for item in catalog["blueprints"]
            if item["id"] == "document_translation_review"
        )
        self.assertEqual(translation["document"]["type"], "translatable_document")
        self.assertEqual(
            translation["additional_document_types"][0]["id"],
            "translated_document",
        )
        self.assertEqual(
            translation["additional_document_relations"][0]["id"],
            "translated",
        )
        self.assertEqual(translation["manual_steps"], ["review_translation"])
        self.assertEqual(translation["stream_count"], 3)
        self.assertIn("translation_pool", translation["resource_pools"])
        self.assertIn("render_pool", translation["resource_pools"])
        translation_steps = {step["id"]: step for step in translation["steps"]}
        self.assertEqual(
            translation_steps["ingest"]["accepts_document_types"],
            ["translatable_document", "translated_document"],
        )
        self.assertEqual(
            translation_steps["segment_text"]["operation_type"],
            "split",
        )
        self.assertEqual(
            translation_steps["translate_segments"]["operation_type"],
            "translate",
        )
        self.assertEqual(
            translation_steps["translate_segments"]["streams"][0]["stream_id"],
            "translations",
        )
        self.assertEqual(
            translation_steps["assemble_translation"]["emits_document_types"],
            ["translated_document"],
        )

        selected = _run_cli(
            "scaffold-blueprints",
            "--blueprint",
            "llm_document_processing",
        )
        blueprint = selected["blueprint"]
        self.assertEqual(blueprint["document"]["type"], "llm_document")
        self.assertEqual(blueprint["manual_steps"], ["review"])
        self.assertIn("llm_pool", blueprint["resource_pools"])
        steps = {step["id"]: step for step in blueprint["steps"]}
        self.assertEqual(steps["generate"]["capability"], "generate_response")
        self.assertEqual(steps["generate"]["streams"][0]["stream_id"], "tokens")
        self.assertEqual(steps["review"]["policy"]["adapter_kind"], "manual")
        self.assertEqual(
            [item["id"] for item in list_scaffold_blueprints()],
            [item["id"] for item in catalog["blueprints"]],
        )
        public_blueprint = get_scaffold_blueprint("llm_document_processing")
        self.assertIsNotNone(public_blueprint)
        assert public_blueprint is not None
        self.assertEqual(public_blueprint.document_type, "llm_document")
        mapped = scaffold_blueprint_from_mapping(
            {
                "id": "mapped_document_flow",
                "document": {"type": "mapped_document"},
                "steps": [
                    {
                        "id": "ingest",
                        "capability": "ingest_document",
                        "artifact_kind": "source_payload",
                    }
                ],
            }
        )
        self.assertEqual(mapped.id, "mapped_document_flow")
        self.assertEqual(mapped.steps, ("ingest",))

        kb = _run_cli(
            "scaffold-blueprints",
            "--blueprint",
            "knowledge_base_ingestion",
        )["blueprint"]
        self.assertEqual(kb["document"]["type"], "knowledge_document")
        self.assertEqual(kb["step_count"], 6)
        self.assertEqual(kb["stream_count"], 3)
        self.assertIn("embedding_pool", kb["resource_pools"])
        self.assertIn("index_pool", kb["resource_pools"])
        kb_steps = {step["id"]: step for step in kb["steps"]}
        self.assertEqual(kb_steps["index"]["capability"], "index_chunks")

        structured = _run_cli(
            "scaffold-blueprints",
            "--blueprint",
            "structured_extraction_review",
        )["blueprint"]
        self.assertEqual(structured["document"]["type"], "structured_document")
        self.assertEqual(structured["manual_steps"], ["review"])
        self.assertIn("llm_pool", structured["resource_pools"])
        structured_steps = {step["id"]: step for step in structured["steps"]}
        self.assertEqual(
            structured_steps["extract_fields"]["streams"][0]["stream_id"],
            "fields",
        )
        self.assertEqual(structured_steps["review"]["policy"]["adapter_kind"], "manual")

        tabular = _run_cli(
            "scaffold-blueprints",
            "--blueprint",
            "tabular_data_processing",
        )["blueprint"]
        self.assertEqual(tabular["document"]["type"], "tabular_document")
        self.assertEqual(tabular["step_count"], 7)
        self.assertEqual(tabular["stream_count"], 4)
        self.assertIn("tabular_pool", tabular["resource_pools"])
        self.assertIn("llm_pool", tabular["resource_pools"])
        tabular_steps = {step["id"]: step for step in tabular["steps"]}
        self.assertEqual(
            [operation["id"] for operation in tabular["operation_types"]],
            [
                "ingest",
                "analyze",
                "normalize",
                "validate",
                "enrich",
                "aggregate",
                "export",
            ],
        )
        self.assertEqual(
            tabular_steps["profile_table"]["streams"][0]["stream_id"],
            "rows",
        )
        self.assertEqual(
            tabular_steps["normalize_rows"]["streams"][0]["consumers"],
            ["validate_rows", "enrich_records"],
        )
        self.assertEqual(
            tabular_steps["validate_rows"]["streams"][0]["stream_id"],
            "validation_issues",
        )

    def test_document_digitalization_blueprint_splits_parent_and_page_paths(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "digitalization"
            _run_cli(
                "scaffold",
                "--output-dir",
                str(package_dir),
                "--package-id",
                "digitalization_pkg",
                "--pipeline-id",
                "digitalization_flow",
                "--blueprint",
                "document_digitalization",
                "--adapter-kind",
                "queue",
            )
            registry = PipelineRegistry.from_directory(root)
            service = RuntimeService(
                registry=registry,
                store=InMemoryStateStore(),
            )

            def output_for(process_id: str, document_id: str) -> ProcessOutput:
                return ProcessOutput(
                    values={
                        "status": "ok",
                        "process_id": process_id,
                        "document_id": document_id,
                    }
                )

            async def exercise() -> tuple[dict, dict, dict, dict, dict]:
                schedules = await service.initialize_documents(
                    run_id="run_digitalization_split",
                    documents=[
                        RuntimeDocumentInput(
                            document_id="contract.pdf",
                            pipeline_id="digitalization_flow",
                            document_type="generic_document",
                            media_type="application/pdf",
                            source_uri="file:///tmp/contract.pdf",
                        )
                    ],
                )
                _ingest_output, _refreshed, parent_after_ingest, _spawned = (
                    await service.complete_process_output(
                        run_id="run_digitalization_split",
                        document_id="contract.pdf",
                        pipeline_id="digitalization_flow",
                        process_id="ingest",
                        output=output_for("ingest", "contract.pdf"),
                    )
                )
                extract_output, _refreshed, parent_after_extract, spawned = (
                    await service.complete_process_output(
                        run_id="run_digitalization_split",
                        document_id="contract.pdf",
                        pipeline_id="digitalization_flow",
                        process_id="extract",
                        output=ProcessOutput(
                            values={
                                "status": "ok",
                                "process_id": "extract",
                                "document_id": "contract.pdf",
                            },
                            spawn_documents=[
                                SpawnDocumentInput(
                                    document_id="contract.pdf#page-1",
                                    relation="page",
                                    media_type="application/pdf",
                                    source_uri="file:///tmp/contract-page-1.pdf",
                                    values={"source": "contract-page-1.pdf"},
                                    metadata={"page_number": 1},
                                )
                            ],
                        ),
                    )
                )
                _page_ingest_output, _refreshed, page_after_ingest, _spawned = (
                    await service.complete_process_output(
                        run_id="run_digitalization_split",
                        document_id="contract.pdf#page-1",
                        pipeline_id="digitalization_flow",
                        process_id="ingest",
                        output=output_for("ingest", "contract.pdf#page-1"),
                    )
                )
                await service.complete_process_output(
                    run_id="run_digitalization_split",
                    document_id="contract.pdf#page-1",
                    pipeline_id="digitalization_flow",
                    process_id="normalize",
                    output=output_for("normalize", "contract.pdf#page-1"),
                )
                await service.complete_process_output(
                    run_id="run_digitalization_split",
                    document_id="contract.pdf#page-1",
                    pipeline_id="digitalization_flow",
                    process_id="enrich",
                    output=output_for("enrich", "contract.pdf#page-1"),
                )
                await service.complete_process_output(
                    run_id="run_digitalization_split",
                    document_id="contract.pdf#page-1",
                    pipeline_id="digitalization_flow",
                    process_id="export",
                    output=output_for("export", "contract.pdf#page-1"),
                )
                assemble_claim = await service.claim_next(
                    run_id="run_digitalization_split",
                    pipeline_id="digitalization_flow",
                    worker_id="assemble-worker",
                    process_id="assemble",
                    adapter_kind="queue",
                )
                self.assertIsNotNone(assemble_claim)
                assert assemble_claim is not None
                page_document = await service.store.get_document(
                    run_id="run_digitalization_split",
                    document_id="contract.pdf#page-1",
                )
                self.assertIsNotNone(page_document)
                assert page_document is not None
                return (
                    schedules[0].model_dump(mode="json"),
                    parent_after_ingest.model_dump(mode="json"),
                    parent_after_extract.model_dump(mode="json"),
                    spawned[0].model_dump(mode="json"),
                    {
                        "page_document": page_document.model_dump(mode="json"),
                        "page_after_ingest": page_after_ingest.model_dump(mode="json"),
                        "assemble_claim": assemble_claim.model_dump(mode="json"),
                        "spawn_route_report": extract_output.metadata[
                            "process_runtime"
                        ]["spawn_route_report"],
                    },
                )

            (
                initial,
                parent_after_ingest,
                parent_after_extract,
                spawned,
                page_data,
            ) = asyncio.run(exercise())

            self.assertEqual([item["id"] for item in initial["queued"]], ["ingest"])
            self.assertEqual(
                [item["id"] for item in parent_after_ingest["queued"]],
                ["extract"],
            )
            self.assertEqual(
                sorted(parent_after_extract["skipped"]),
                ["enrich", "export", "normalize"],
            )
            self.assertEqual(parent_after_extract["waiting"], ["assemble"])
            self.assertEqual(spawned["pipeline_id"], "digitalization_flow")
            self.assertEqual([item["id"] for item in spawned["queued"]], ["ingest"])
            self.assertEqual(
                page_data["page_document"]["document_type"],
                "page_document",
            )
            self.assertEqual(
                [item["id"] for item in page_data["page_after_ingest"]["queued"]],
                ["normalize"],
            )
            self.assertIn("extract", page_data["page_after_ingest"]["skipped"])
            self.assertIn("assemble", page_data["page_after_ingest"]["skipped"])
            self.assertEqual(
                page_data["assemble_claim"]["process"]["id"],
                "assemble",
            )
            self.assertEqual(
                page_data["assemble_claim"]["document_id"],
                "contract.pdf",
            )
            self.assertEqual(
                page_data["spawn_route_report"]["documents"][0]["final"][
                    "document_type"
                ],
                "page_document",
            )

    def test_email_processing_blueprint_routes_attachments_as_child_documents(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "email"
            _run_cli(
                "scaffold",
                "--output-dir",
                str(package_dir),
                "--package-id",
                "email_pkg",
                "--pipeline-id",
                "email_flow",
                "--blueprint",
                "email_processing",
                "--adapter-kind",
                "queue",
            )
            registry = PipelineRegistry.from_directory(root)
            service = RuntimeService(
                registry=registry,
                store=InMemoryStateStore(),
            )

            def output_for(process_id: str, document_id: str) -> ProcessOutput:
                return ProcessOutput(
                    values={
                        "status": "ok",
                        "process_id": process_id,
                        "document_id": document_id,
                    }
                )

            async def exercise() -> tuple[dict, dict, dict, dict, dict]:
                schedules = await service.initialize_documents(
                    run_id="run_email_attachment",
                    documents=[
                        RuntimeDocumentInput(
                            document_id="message.eml",
                            pipeline_id="email_flow",
                            document_type="email_document",
                            media_type="message/rfc822",
                            source_uri="file:///tmp/message.eml",
                        )
                    ],
                )
                _ingest_output, _refreshed, after_ingest, _spawned = (
                    await service.complete_process_output(
                        run_id="run_email_attachment",
                        document_id="message.eml",
                        pipeline_id="email_flow",
                        process_id="ingest_email",
                        output=output_for("ingest_email", "message.eml"),
                    )
                )
                _parse_output, _refreshed, _after_parse, _spawned = (
                    await service.complete_process_output(
                        run_id="run_email_attachment",
                        document_id="message.eml",
                        pipeline_id="email_flow",
                        process_id="parse_message",
                        output=output_for("parse_message", "message.eml"),
                    )
                )
                extract_output, _refreshed, _after_extract, spawned = (
                    await service.complete_process_output(
                        run_id="run_email_attachment",
                        document_id="message.eml",
                        pipeline_id="email_flow",
                        process_id="extract_attachments",
                        output=ProcessOutput(
                            values={
                                "status": "ok",
                                "process_id": "extract_attachments",
                                "document_id": "message.eml",
                            },
                            spawn_documents=[
                                SpawnDocumentInput(
                                    document_id="message.eml#attachment-1",
                                    media_type="application/pdf",
                                    source_uri="file:///tmp/invoice.pdf",
                                    values={
                                        "source": "invoice.pdf",
                                        "filename": "invoice.pdf",
                                    },
                                    metadata={
                                        "filename": "invoice.pdf",
                                        "message_id": "message.eml",
                                    },
                                )
                            ],
                        ),
                    )
                )
                _attachment_ingest, _refreshed, attachment_after_ingest, _spawned = (
                    await service.complete_process_output(
                        run_id="run_email_attachment",
                        document_id="message.eml#attachment-1",
                        pipeline_id="email_flow",
                        process_id="ingest_email",
                        output=output_for(
                            "ingest_email",
                            "message.eml#attachment-1",
                        ),
                    )
                )
                attachment = await service.store.get_document(
                    run_id="run_email_attachment",
                    document_id="message.eml#attachment-1",
                )
                self.assertIsNotNone(attachment)
                assert attachment is not None
                return (
                    schedules[0].model_dump(mode="json"),
                    after_ingest.model_dump(mode="json"),
                    spawned[0].model_dump(mode="json"),
                    attachment_after_ingest.model_dump(mode="json"),
                    {
                        "attachment": attachment.model_dump(mode="json"),
                        "spawn_route_report": extract_output.metadata[
                            "process_runtime"
                        ]["spawn_route_report"],
                    },
                )

            initial, after_ingest, spawned, attachment_after_ingest, data = (
                asyncio.run(exercise())
            )

            self.assertEqual([item["id"] for item in initial["queued"]], ["ingest_email"])
            self.assertEqual(
                sorted(item["id"] for item in after_ingest["queued"]),
                ["classify", "parse_message"],
            )
            self.assertEqual(spawned["pipeline_id"], "email_flow")
            self.assertEqual([item["id"] for item in spawned["queued"]], ["ingest_email"])
            self.assertEqual(
                data["attachment"]["document_type"],
                "email_attachment_document",
            )
            self.assertEqual(
                [item["id"] for item in attachment_after_ingest["queued"]],
                ["classify"],
            )
            self.assertIn("parse_message", attachment_after_ingest["skipped"])
            self.assertIn("extract_attachments", attachment_after_ingest["skipped"])
            self.assertEqual(
                data["spawn_route_report"]["documents"][0]["final"]["document_type"],
                "email_attachment_document",
            )

    def test_runtime_cli_scaffolds_custom_blueprint_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipeline_root = root / "pipelines"
            blueprint_file = root / "invoice-blueprint.yaml"
            package_dir = pipeline_root / "invoice_package"
            blueprint_file.write_text(
                textwrap.dedent(
                    """
                    id: invoice_review
                    title: Invoice review
                    document:
                      type: invoice_document
                      media_types: [application/pdf]
                      extensions: [.pdf]
                      value_schema:
                        type: object
                        properties:
                          source:
                            type: string
                          vendor:
                            type: string
                        additionalProperties: true
                      metadata_schema:
                        type: object
                        properties:
                          tenant:
                            type: string
                        additionalProperties: true
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
                        title: Approve
                        category: quality
                    steps:
                      - id: ingest
                        capability: ingest_invoice
                        artifact_kind: source_invoice
                        accepts_document_types: [invoice_document, invoice_page]
                        artifact_media_types: [application/json]
                        artifact_extensions: [.json]
                      - id: extract
                        capability: extract_invoice
                        artifact_kind: extracted_invoice
                        emits_document_types: [invoice_page]
                        artifact_media_types: [application/json, text/plain]
                        streams:
                          - stream_id: pages
                            kinds: [page]
                            max_buffered_chunks: 32
                            value_schema:
                              type: object
                              required: [text]
                              properties:
                                text:
                                  type: string
                              additionalProperties: true
                        policy:
                          priority: 40
                          max_concurrency: 2
                          retry:
                            max_attempts: 3
                            delay_seconds: 10
                            retry_error_kinds: [transient_io]
                        guidance:
                          role: OCR extractor
                          intent: Extract invoice page text for accounting review.
                          replace_sample_with:
                            - Call OCR or document parser.
                            - Emit page stream chunks.
                      - id: approve
                        capability: approve_invoice
                        operation_type: approve
                        artifact_kind: approval
                        needs: [extract]
                        policy:
                          adapter:
                            kind: manual
                          priority: 10
                      - id: triage
                        capability: triage_invoice
                        artifact_kind: triage_report
                        needs: []
                      - id: export
                        capability: export_invoice
                        artifact_kind: exported_invoice
                        needs: [extract, approve, triage]
                        artifact_media_types: [application/json]
                    """
                ).strip(),
                encoding="utf-8",
            )

            inspected = _run_cli(
                "scaffold-blueprints",
                "--blueprint-file",
                str(blueprint_file),
            )
            self.assertEqual(inspected["source"], str(blueprint_file))
            self.assertEqual(inspected["blueprint"]["id"], "invoice_review")
            self.assertEqual(inspected["blueprint"]["document"]["type"], "invoice_document")
            self.assertEqual(
                inspected["blueprint"]["additional_document_types"][0]["id"],
                "invoice_page",
            )
            self.assertEqual(
                inspected["blueprint"]["additional_document_relations"][0]["id"],
                "page",
            )
            self.assertEqual(
                inspected["blueprint"]["operation_types"][0]["id"],
                "approve",
            )
            self.assertEqual(inspected["blueprint"]["manual_steps"], ["approve"])
            self.assertIn("--blueprint-file", inspected["blueprint"]["scaffold_command"])
            inspected_steps = {
                step["id"]: step for step in inspected["blueprint"]["steps"]
            }
            self.assertEqual(inspected_steps["ingest"]["needs"], [])
            self.assertEqual(inspected_steps["extract"]["needs"], ["ingest"])
            self.assertEqual(
                inspected_steps["extract"]["guidance"]["role"],
                "OCR extractor",
            )
            self.assertIn(
                "document parser",
                inspected_steps["extract"]["guidance"]["replace_sample_with"][0],
            )
            self.assertEqual(inspected_steps["approve"]["needs"], ["extract"])
            self.assertEqual(inspected_steps["approve"]["operation_type"], "approve")
            self.assertEqual(inspected_steps["triage"]["needs"], [])
            self.assertEqual(
                inspected_steps["export"]["needs"],
                ["extract", "approve", "triage"],
            )

            scaffold = _run_cli(
                "scaffold",
                "--output-dir",
                str(package_dir),
                "--package-id",
                "invoice_package",
                "--pipeline-id",
                "invoice_flow",
                "--blueprint-file",
                str(blueprint_file),
                "--adapter-kind",
                "queue",
            )
            self.assertEqual(scaffold["blueprint"], "invoice_review")
            self.assertEqual(scaffold["document_type"], "invoice_document")
            self.assertEqual(
                scaffold["step_ids"],
                ["ingest", "extract", "approve", "triage", "export"],
            )
            self.assertEqual(
                scaffold["needs_by_step"],
                {
                    "ingest": [],
                    "extract": ["ingest"],
                    "approve": ["extract"],
                    "triage": [],
                    "export": ["extract", "approve", "triage"],
                },
            )

            package_text = (package_dir / "process-runtime-package.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn("id: invoice_document", package_text)
            self.assertIn("id: invoice_page", package_text)
            self.assertIn("document_relations:", package_text)
            self.assertIn("source_document_types:", package_text)
            self.assertIn("target_document_types:", package_text)
            self.assertIn("operation_types:", package_text)
            self.assertIn("operation_type: approve", package_text)
            self.assertIn(
                'accepts_document_types: ["invoice_document", "invoice_page"]',
                package_text,
            )
            self.assertIn('emits_document_types: ["invoice_page"]', package_text)
            self.assertIn("id: extracted_invoice", package_text)
            self.assertIn("stream: pages", package_text)
            pipeline_text = (package_dir / "invoice_flow.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn("capability: extract_invoice", pipeline_text)
            self.assertIn("kind: manual", pipeline_text)
            self.assertIn('needs: ["extract", "approve", "triage"]', pipeline_text)
            extract_step_text = (package_dir / "steps" / "extract.py").read_text(
                encoding="utf-8"
            )
            self.assertIn("'role': 'OCR extractor'", extract_step_text)
            self.assertIn(
                "'intent': 'Extract invoice page text for accounting review.'",
                extract_step_text,
            )

            validate = _run_cli(
                "--pipeline-dir",
                str(pipeline_root),
                "validate",
                "--json",
                "--check-commands",
            )
            self.assertTrue(validate["ok"])
            self.assertEqual(
                validate["packages"][0]["document_types"][0]["id"],
                "invoice_document",
            )
            self.assertEqual(
                validate["packages"][0]["document_types"][1]["id"],
                "invoice_page",
            )
            capabilities = {
                capability["id"]: capability
                for capability in validate["packages"][0]["capabilities"]
            }
            self.assertEqual(capabilities["approve_invoice"]["operation_type"], "approve")
            self.assertEqual(
                capabilities["triage_invoice"]["accepts_document_types"],
                ["invoice_document"],
            )
            self.assertEqual(
                capabilities["triage_invoice"]["accepts_artifact_kinds"],
                [],
            )
            self.assertEqual(
                capabilities["export_invoice"]["accepts_artifact_kinds"],
                ["extracted_invoice", "approval", "triage_report"],
            )

            with self.assertRaisesRegex(ValueError, "depends on unknown step"):
                scaffold_blueprint_from_mapping(
                    {
                        "id": "bad_invoice",
                        "document": {"type": "invoice_document"},
                        "steps": [
                            {
                                "id": "ingest",
                                "capability": "ingest_invoice",
                                "artifact_kind": "source_invoice",
                                "needs": ["missing_step"],
                            }
                        ],
                    }
                )

    def test_runtime_cli_scaffolds_named_blueprint_workflow_package(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "media_package"
            scaffold = _run_cli(
                "scaffold",
                "--output-dir",
                str(package_dir),
                "--package-id",
                "media_package",
                "--pipeline-id",
                "media_flow",
                "--blueprint",
                "generative_media",
                "--adapter-kind",
                "queue",
            )
            self.assertEqual(scaffold["blueprint"], "generative_media")
            self.assertEqual(scaffold["document_type"], "creative_brief")
            self.assertEqual(
                scaffold["step_ids"],
                ["ingest_brief", "plan", "generate_assets", "render", "export"],
            )
            self.assertTrue((package_dir / "steps" / "generate_assets.py").exists())

            package_text = (package_dir / "process-runtime-package.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn("id: creative_brief", package_text)
            self.assertIn('media_types: ["text/plain", "application/json"]', package_text)
            self.assertIn("id: generated_assets", package_text)
            self.assertIn("metadata_schema:", package_text)
            self.assertIn("project_id:", package_text)
            self.assertIn(
                'media_types: ["application/json", "image/*", "audio/*", "video/*"]',
                package_text,
            )
            self.assertIn(
                'extensions: [".json", ".png", ".jpg", ".wav", ".mp4"]',
                package_text,
            )
            self.assertIn("id: generate_assets", package_text)
            self.assertIn('capabilities: ["generate_assets"]', package_text)
            self.assertIn("output_schema:", package_text)
            self.assertIn("emits_streams:", package_text)
            self.assertIn("stream: assets", package_text)
            self.assertIn("stream: frames", package_text)

            pipeline_text = (package_dir / "media_flow.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn("capability: ingest_brief", pipeline_text)
            self.assertIn("capability: generate_assets", pipeline_text)
            self.assertIn('queue: "media_package.generate_assets"', pipeline_text)

            validate = _run_cli(
                "--pipeline-dir",
                str(root),
                "validate",
                "--json",
                "--check-commands",
            )
            self.assertTrue(validate["ok"])
            self.assertEqual(
                validate["packages"][0]["document_types"][0]["id"],
                "creative_brief",
            )
            self.assertEqual(
                [item["id"] for item in validate["packages"][0]["artifact_kinds"]],
                [
                    "creative_brief_payload",
                    "generation_plan",
                    "generated_assets",
                    "rendered_media",
                    "exported_media",
                ],
            )
            artifact_extensions = {
                item["id"]: item["extensions"]
                for item in validate["packages"][0]["artifact_kinds"]
            }
            self.assertEqual(
                artifact_extensions["generated_assets"],
                [".json", ".png", ".jpg", ".wav", ".mp4"],
            )
            self.assertEqual(
                validate["pipelines"][0]["steps"][2]["capability"],
                "generate_assets",
            )
            source_list_text = (package_dir / "source-list.example.csv").read_text(
                encoding="utf-8"
            )
            self.assertIn("metadata.project_id", source_list_text)

            db_path = root / "runtime.db"
            created = _run_cli(
                "--pipeline-dir",
                str(root),
                "create-run",
                "--db",
                str(db_path),
                "--pipeline",
                "media_flow",
                "--run-id",
                "run_media_blueprint",
                "--document-type",
                "creative_brief",
                "--media-type",
                "text/plain",
                "--document",
                "brief=file:///tmp/brief.txt",
            )
            self.assertEqual(created["run"]["status"], "queued")
            self.assertEqual(created["schedules"][0]["queued"][0]["id"], "ingest_brief")

            claim = _run_cli(
                "--pipeline-dir",
                str(root),
                "claim",
                "--db",
                str(db_path),
                "--pipeline",
                "media_flow",
                "--run-id",
                "run_media_blueprint",
                "--worker-id",
                "media-worker",
                "--adapter-kind",
                "queue",
                "--capability",
                "ingest_brief",
            )
            self.assertEqual(claim["claim"]["process"]["id"], "ingest_brief")
            self.assertEqual(claim["claim"]["process"]["capability"], "ingest_brief")

    def test_runtime_cli_scaffolds_llm_document_processing_blueprint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "llm_package"
            scaffold = _run_cli(
                "scaffold",
                "--output-dir",
                str(package_dir),
                "--package-id",
                "llm_package",
                "--pipeline-id",
                "llm_flow",
                "--blueprint",
                "llm_document_processing",
            )
            self.assertEqual(scaffold["blueprint"], "llm_document_processing")
            self.assertEqual(scaffold["document_type"], "llm_document")
            self.assertEqual(
                scaffold["step_ids"],
                [
                    "ingest",
                    "extract_text",
                    "chunk",
                    "embed",
                    "retrieve",
                    "generate",
                    "review",
                    "export",
                ],
            )
            self.assertTrue((package_dir / "steps" / "generate.py").exists())
            extract_source = (package_dir / "steps" / "extract_text.py").read_text(
                encoding="utf-8"
            )
            self.assertIn("STREAMS =", extract_source)
            self.assertIn("stream_chunk(", extract_source)

            package_text = (package_dir / "process-runtime-package.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn("id: llm_document", package_text)
            self.assertIn('extensions: [".pdf", ".eml", ".md", ".txt"]', package_text)
            self.assertIn("id: document_chunks", package_text)
            self.assertIn("id: chunk_document", package_text)
            self.assertIn("value_schema:", package_text)
            self.assertIn("output_schema:", package_text)
            self.assertIn("emits_streams:", package_text)
            self.assertIn("stream: pages", package_text)
            self.assertIn("stream: chunks", package_text)
            self.assertIn("stream: tokens", package_text)
            self.assertIn("max_buffered_chunks:", package_text)
            self.assertIn("const: chunk", package_text)
            self.assertIn("question:", package_text)
            self.assertTrue(
                (
                    package_dir
                    / "contracts"
                    / "streams"
                    / "generate_response.streams.yaml"
                ).exists()
            )
            self.assertTrue(
                (
                    package_dir
                    / "contracts"
                    / "policies"
                    / "review.policy.yaml"
                ).exists()
            )
            review_policy_text = (
                package_dir / "contracts" / "policies" / "review.policy.yaml"
            ).read_text(encoding="utf-8")
            self.assertIn("kind: manual", review_policy_text)
            generate_policy_text = (
                package_dir / "contracts" / "policies" / "generate.policy.yaml"
            ).read_text(encoding="utf-8")
            self.assertIn("resource_pool: llm_pool", generate_policy_text)
            run_input_text = (package_dir / "run-input.example.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn("pipeline_id: llm_flow", run_input_text)
            self.assertIn("document_type: llm_document", run_input_text)
            self.assertIn("media_type: application/pdf", run_input_text)
            self.assertIn("source_uri: file://", run_input_text)
            self.assertIn("/incoming/sample.pdf", run_input_text)
            self.assertIn("blueprint: llm_document_processing", run_input_text)
            self.assertIn("question: sample question", run_input_text)
            self.assertIn("task: sample task", run_input_text)
            self.assertIn("language: sample language", run_input_text)
            self.assertIn("collection: sample collection", run_input_text)
            source_list_text = (package_dir / "source-list.example.csv").read_text(
                encoding="utf-8"
            )
            self.assertIn("value.question", source_list_text)
            self.assertIn("metadata.collection", source_list_text)
            self.assertIn("application/pdf", source_list_text)
            readme_text = (package_dir / "README.scaffold.md").read_text(
                encoding="utf-8"
            )
            self.assertIn("stream_chunk", readme_text)
            self.assertIn("backpressure", readme_text)
            self.assertIn("| review | manual | review_output | generate | 10 | - | default |", readme_text)
            self.assertIn("Complete a manual gate", readme_text)

            validate = _run_cli(
                "--pipeline-dir",
                str(root),
                "validate",
                "--json",
                "--check-commands",
            )
            self.assertTrue(validate["ok"])
            self.assertEqual(
                [item["id"] for item in validate["packages"][0]["artifact_kinds"]],
                [
                    "source_payload",
                    "extracted_text",
                    "document_chunks",
                    "chunk_embeddings",
                    "retrieval_context",
                    "generated_response",
                    "review_decision",
                    "exported_result",
                ],
            )
            artifact_extensions = {
                item["id"]: item["extensions"]
                for item in validate["packages"][0]["artifact_kinds"]
            }
            self.assertEqual(
                artifact_extensions["extracted_text"],
                [".json", ".txt", ".md"],
            )
            self.assertEqual(
                artifact_extensions["chunk_embeddings"],
                [".json", ".ndjson"],
            )
            self.assertEqual(
                validate["packages"][0]["capabilities"][2]["output_schema"][
                    "properties"
                ]["process_id"]["const"],
                "chunk",
            )
            self.assertEqual(
                validate["packages"][0]["capabilities"][1]["emits_streams"][0][
                    "stream_id"
                ],
                "pages",
            )
            self.assertEqual(
                validate["packages"][0]["capabilities"][5]["emits_streams"][0][
                    "stream_id"
                ],
                "tokens",
            )
            steps_by_id = {
                step["id"]: step
                for step in validate["pipelines"][0]["steps"]
            }
            self.assertEqual(steps_by_id["review"]["adapter_kind"], "manual")
            self.assertEqual(steps_by_id["extract_text"]["resource_pool"], "ocr_pool")
            self.assertEqual(steps_by_id["extract_text"]["priority"], 50)
            self.assertEqual(steps_by_id["embed"]["resource_pool"], "embedding_pool")
            self.assertEqual(steps_by_id["generate"]["resource_pool"], "llm_pool")
            sample_run_input = _run_cli(
                "--pipeline-dir",
                str(root),
                "validate-run",
                "--run-input",
                str(package_dir / "run-input.example.yaml"),
            )
            self.assertTrue(sample_run_input["ok"])
            self.assertEqual(
                sample_run_input["documents"][0]["media_type"],
                "application/pdf",
            )
            self.assertEqual(
                sample_run_input["documents"][0]["value_keys"],
                ["language", "question", "source", "task"],
            )

            db_path = root / "runtime.db"
            created = _run_cli(
                "--pipeline-dir",
                str(root),
                "create-run",
                "--db",
                str(db_path),
                "--pipeline",
                "llm_flow",
                "--run-id",
                "run_llm_blueprint",
                "--document-type",
                "llm_document",
                "--media-type",
                "text/plain",
                "--document",
                "doc.txt=file:///tmp/doc.txt",
            )
            self.assertEqual(created["schedules"][0]["queued"][0]["id"], "ingest")

            chunk = _run_cli(
                "--pipeline-dir",
                str(root),
                "stream-append",
                "--db",
                str(db_path),
                "--run-id",
                "run_llm_blueprint",
                "--document-id",
                "doc.txt",
                "--process-id",
                "extract_text",
                "--stream-id",
                "pages",
                "--kind",
                "page",
                "--value",
                "text=page text",
            )
            self.assertEqual(chunk["chunk"]["stream_id"], "pages")
            self.assertEqual(chunk["chunk"]["kind"], "page")

            code, bad_chunk = _run_cli_raw(
                "--pipeline-dir",
                str(root),
                "stream-append",
                "--db",
                str(db_path),
                "--run-id",
                "run_llm_blueprint",
                "--document-id",
                "doc.txt",
                "--process-id",
                "extract_text",
                "--stream-id",
                "pages",
                "--kind",
                "section",
                "--value",
                "text=bad",
            )
            self.assertNotEqual(code, 0)
            self.assertIn("kind 'section'", bad_chunk["error"])

            worked = _run_cli(
                "--pipeline-dir",
                str(root),
                "run-until-idle",
                "--db",
                str(db_path),
                "--pipeline",
                "llm_flow",
                "--run-id",
                "run_llm_blueprint",
                "--worker-id",
                "worker-llm",
                "--adapter-kind",
                "subprocess",
            )
            self.assertEqual(worked["completed_count"], 6)
            document = worked["state"]["documents"][0]
            self.assertEqual(document["statuses"]["review"], "waiting")
            self.assertEqual(document["statuses"]["export"], "waiting")
            extract_step = next(
                step for step in document["steps"] if step["id"] == "extract_text"
            )
            self.assertGreaterEqual(extract_step["stream_chunk_count"], 2)
            self.assertEqual(extract_step["streams"][0]["stream_id"], "pages")
            generate_step = next(
                step for step in document["steps"] if step["id"] == "generate"
            )
            self.assertEqual(generate_step["streams"][0]["stream_id"], "tokens")

            completed_review = _run_cli(
                "--pipeline-dir",
                str(root),
                "complete-process",
                "--db",
                str(db_path),
                "--pipeline",
                "llm_flow",
                "--run-id",
                "run_llm_blueprint",
                "--document-id",
                "doc.txt",
                "--process-id",
                "review",
                "--worker-id",
                "operator",
                "--value",
                "status=ok",
                "--value",
                "process_id=review",
                "--value",
                "document_id=doc.txt",
            )
            self.assertIn("review", completed_review["schedule"]["completed"])

            exported = _run_cli(
                "--pipeline-dir",
                str(root),
                "run-until-idle",
                "--db",
                str(db_path),
                "--pipeline",
                "llm_flow",
                "--run-id",
                "run_llm_blueprint",
                "--worker-id",
                "worker-llm",
                "--adapter-kind",
                "subprocess",
            )
            self.assertEqual(exported["completed_count"], 1)
            document = exported["state"]["documents"][0]
            self.assertEqual(document["statuses"]["review"], "completed")
            self.assertEqual(document["statuses"]["export"], "completed")
            self.assertTrue(document["projections"]["workflow_result"]["complete"])

    def test_runtime_cli_scaffold_blueprint_step_policy_overrides_deep_merge(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "llm_policy_package"
            generate_policy = root / "generate-policy.yaml"
            generate_policy.write_text(
                textwrap.dedent(
                    """
                    resources:
                      memory_mb: 8192
                      labels: [llm]
                    retry:
                      max_attempts: 5
                    config:
                      model: local-test
                    """
                ).strip(),
                encoding="utf-8",
            )

            _run_cli(
                "scaffold",
                "--output-dir",
                str(package_dir),
                "--package-id",
                "llm_policy_package",
                "--pipeline-id",
                "llm_policy_flow",
                "--blueprint",
                "llm_document_processing",
                "--step-policy",
                f"generate={generate_policy}",
            )

            pipeline_text = (package_dir / "llm_policy_flow.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn("priority: 20", pipeline_text)
            self.assertIn("max_concurrency: 2", pipeline_text)
            self.assertIn("resource_pool: llm_pool", pipeline_text)
            self.assertIn("max_attempts: 5", pipeline_text)
            self.assertIn("delay_seconds: 60", pipeline_text)
            self.assertIn("retry_error_kinds:", pipeline_text)
            self.assertIn("- rate_limited", pipeline_text)
            self.assertIn("memory_mb: 8192", pipeline_text)
            self.assertIn("- llm", pipeline_text)
            self.assertIn("model: local-test", pipeline_text)

            validate = _run_cli(
                "--pipeline-dir",
                str(root),
                "validate",
                "--json",
                "--check-commands",
            )
            self.assertTrue(validate["ok"])
            generate = next(
                step
                for step in validate["pipelines"][0]["steps"]
                if step["id"] == "generate"
            )
            self.assertEqual(generate["priority"], 20)
            self.assertEqual(generate["max_concurrency"], 2)
            self.assertEqual(generate["resource_pool"], "llm_pool")

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

    def test_runtime_cli_checks_package_worker_commands_relative_to_package(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "package"
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
                        command: ["python", "steps/enrich.py"]
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
            steps_dir = package_dir / "steps"
            steps_dir.mkdir()
            (steps_dir / "enrich.py").write_text(
                "from fala.sdk import run_stdio\n"
                "run_stdio(lambda ctx: {'values': {'ok': True}})\n",
                encoding="utf-8",
            )

            checked = _run_cli(
                "--pipeline-dir",
                str(root),
                "validate",
                "--json",
                "--check-commands",
            )

        self.assertTrue(checked["ok"])
        self.assertEqual(checked["command_issues"], [])

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
            self.assertIn("stream_chunks", schema["schema"]["properties"])
            output_stream_schema = _run_cli("schema", "process-output-stream-chunk")
            self.assertEqual(output_stream_schema["model"], "process-output-stream-chunk")
            self.assertIn("stream_id", output_stream_schema["schema"]["properties"])
            pipeline_schema = _run_cli("schema", "pipeline")
            self.assertIn("input_values", pipeline_schema["schema"]["properties"])
            package_schema = _run_cli("schema", "workflow-package")
            self.assertEqual(package_schema["model"], "workflow-package")
            self.assertIn("pipelines", package_schema["schema"]["properties"])
            self.assertIn("workers", package_schema["schema"]["properties"])
            package_release_schema = _run_cli("schema", "workflow-package-release")
            self.assertEqual(
                package_release_schema["model"],
                "workflow-package-release",
            )
            self.assertIn("contract_sha256", package_release_schema["schema"]["properties"])
            registry_index_schema = _run_cli("schema", "workflow-registry-index")
            self.assertEqual(registry_index_schema["model"], "workflow-registry-index")
            self.assertIn("packages", registry_index_schema["schema"]["properties"])
            readiness_schema = _run_cli("schema", "workflow-readiness-report")
            self.assertEqual(readiness_schema["model"], "workflow-readiness-report")
            self.assertIn("packages", readiness_schema["schema"]["properties"])
            package_readiness_schema = _run_cli(
                "schema",
                "workflow-package-readiness",
            )
            self.assertEqual(
                package_readiness_schema["model"],
                "workflow-package-readiness",
            )
            self.assertIn(
                "issues",
                package_readiness_schema["schema"]["properties"],
            )
            workflow_secret_schema = _run_cli("schema", "workflow-secret")
            self.assertEqual(workflow_secret_schema["model"], "workflow-secret")
            self.assertIn("env_var", workflow_secret_schema["schema"]["properties"])
            sandbox_schema = _run_cli("schema", "worker-sandbox")
            self.assertEqual(sandbox_schema["model"], "worker-sandbox")
            self.assertIn("read_only_root_filesystem", sandbox_schema["schema"]["properties"])
            artifact_schema = _run_cli("schema", "artifact")
            self.assertIn("pattern", artifact_schema["schema"]["properties"]["id"])
            resource_schema = _run_cli("schema", "resource")
            self.assertEqual(resource_schema["model"], "resource")
            self.assertIn("gpu_count", resource_schema["schema"]["properties"])
            self.assertIn("labels", resource_schema["schema"]["properties"])
            resource_pool_schema = _run_cli("schema", "resource-pool")
            self.assertEqual(resource_pool_schema["model"], "resource-pool")
            self.assertIn("resources", resource_pool_schema["schema"]["properties"])
            resource_quantity_schema = _run_cli("schema", "resource-quantity")
            self.assertEqual(resource_quantity_schema["model"], "resource-quantity")
            self.assertIn("memory_mb", resource_quantity_schema["schema"]["properties"])
            action_schema = _run_cli("schema", "process-action")
            self.assertEqual(action_schema["model"], "process-action")
            self.assertIn("action", action_schema["schema"]["properties"])
            self.assertIn("reason", action_schema["schema"]["properties"])
            event_page_schema = _run_cli("schema", "event-page")
            self.assertEqual(event_page_schema["model"], "event-page")
            self.assertIn("events", event_page_schema["schema"]["properties"])
            self.assertIn("next_after_event_id", event_page_schema["schema"]["properties"])
            event_schema = _run_cli("schema", "event")
            self.assertEqual(event_schema["model"], "event")
            self.assertIn("operation_type", event_schema["schema"]["properties"])
            audit_event_schema = _run_cli("schema", "operator-audit-event")
            self.assertEqual(audit_event_schema["model"], "operator-audit-event")
            self.assertIn("actor", audit_event_schema["schema"]["properties"])
            audit_page_schema = _run_cli("schema", "operator-audit-event-page")
            self.assertEqual(audit_page_schema["model"], "operator-audit-event-page")
            self.assertIn("events", audit_page_schema["schema"]["properties"])
            runtime_state_schema = _run_cli("schema", "runtime-state")
            self.assertEqual(runtime_state_schema["model"], "runtime-state")
            self.assertIn("summary", runtime_state_schema["schema"]["properties"])
            self.assertIn("documents", runtime_state_schema["schema"]["properties"])
            self.assertIn(
                "operation_type_counts",
                runtime_state_schema["schema"]["$defs"]["RuntimeStateSummary"][
                    "properties"
                ],
            )
            runtime_step_report_schema = _run_cli("schema", "runtime-step-report")
            self.assertEqual(
                runtime_step_report_schema["model"],
                "runtime-step-report",
            )
            self.assertIn(
                "steps",
                runtime_step_report_schema["schema"]["properties"],
            )
            process_record_schema = _run_cli("schema", "runtime-process-record")
            self.assertEqual(process_record_schema["model"], "runtime-process-record")
            self.assertIn(
                "operation_type",
                process_record_schema["schema"]["properties"],
            )
            queue_metrics_schema = _run_cli("schema", "runtime-queue-metrics")
            self.assertEqual(queue_metrics_schema["model"], "runtime-queue-metrics")
            self.assertIn("processes", queue_metrics_schema["schema"]["properties"])
            self.assertIn("saturated_process_count", queue_metrics_schema["schema"]["properties"])
            self.assertIn("retry_backoff_count", queue_metrics_schema["schema"]["properties"])
            self.assertIn("missing_worker_count", queue_metrics_schema["schema"]["properties"])
            queue_work_schema = _run_cli("schema", "queue-work-envelope")
            self.assertEqual(queue_work_schema["model"], "queue-work-envelope")
            self.assertIn("context", queue_work_schema["schema"]["properties"])
            self.assertIn("claim_expires_at", queue_work_schema["schema"]["properties"])
            queue_result_schema = _run_cli("schema", "queue-result-envelope")
            self.assertEqual(queue_result_schema["model"], "queue-result-envelope")
            self.assertIn("output", queue_result_schema["schema"]["properties"])
            self.assertIn("status", queue_result_schema["schema"]["properties"])
            run_health_schema = _run_cli("schema", "runtime-run-health")
            self.assertEqual(run_health_schema["model"], "runtime-run-health")
            self.assertIn("status", run_health_schema["schema"]["properties"])
            self.assertIn("issues", run_health_schema["schema"]["properties"])
            artifact_gc_schema = _run_cli("schema", "runtime-artifact-gc-plan")
            self.assertEqual(artifact_gc_schema["model"], "runtime-artifact-gc-plan")
            self.assertIn("orphaned_blobs", artifact_gc_schema["schema"]["properties"])
            retention_schema = _run_cli("schema", "runtime-run-retention-plan")
            self.assertEqual(
                retention_schema["model"],
                "runtime-run-retention-plan",
            )
            self.assertIn("runs", retention_schema["schema"]["properties"])
            stream_chunk_schema = _run_cli("schema", "runtime-stream-chunk")
            self.assertEqual(stream_chunk_schema["model"], "runtime-stream-chunk")
            self.assertIn("sequence", stream_chunk_schema["schema"]["properties"])
            stream_batch_schema = _run_cli("schema", "runtime-stream-batch")
            self.assertEqual(stream_batch_schema["model"], "runtime-stream-batch")
            self.assertIn("chunks", stream_batch_schema["schema"]["properties"])
            self.assertIn("checkpoint", stream_batch_schema["schema"]["properties"])
            stream_schema = _run_cli("schema", "stream")
            self.assertEqual(stream_schema["model"], "stream")
            self.assertIn("value_schema", stream_schema["schema"]["properties"])
            self.assertIn("max_buffered_chunks", stream_schema["schema"]["properties"])
            stream_checkpoint_schema = _run_cli("schema", "runtime-stream-checkpoint")
            self.assertEqual(
                stream_checkpoint_schema["model"],
                "runtime-stream-checkpoint",
            )
            self.assertIn(
                "consumer_id",
                stream_checkpoint_schema["schema"]["properties"],
            )
            stream_snapshot_schema = _run_cli("schema", "runtime-stream-snapshot")
            self.assertEqual(stream_snapshot_schema["model"], "runtime-stream-snapshot")
            self.assertIn("chunk_count", stream_snapshot_schema["schema"]["properties"])
            self.assertIn(
                "max_checkpoint_lag",
                stream_snapshot_schema["schema"]["properties"],
            )
            self.assertIn(
                "checkpoint_sequences",
                stream_snapshot_schema["schema"]["properties"],
            )
            stream_lag_item_schema = _run_cli("schema", "runtime-stream-lag-item")
            self.assertEqual(
                stream_lag_item_schema["model"],
                "runtime-stream-lag-item",
            )
            self.assertIn("lag", stream_lag_item_schema["schema"]["properties"])
            stream_lag_page_schema = _run_cli("schema", "runtime-stream-lag-page")
            self.assertEqual(
                stream_lag_page_schema["model"],
                "runtime-stream-lag-page",
            )
            self.assertIn("items", stream_lag_page_schema["schema"]["properties"])
            trace_schema = _run_cli("schema", "runtime-trace")
            self.assertEqual(trace_schema["model"], "runtime-trace")
            self.assertIn("processes", trace_schema["schema"]["properties"])
            self.assertIn("attempt_count", trace_schema["schema"]["properties"])
            self.assertIn("operation_type", trace_schema["schema"]["properties"])
            self.assertIn(
                "operation_type",
                trace_schema["schema"]["$defs"]["RuntimeProcessTrace"][
                    "properties"
                ],
            )
            worker_state_schema = _run_cli("schema", "runtime-worker-state")
            self.assertEqual(worker_state_schema["model"], "runtime-worker-state")
            self.assertIn("healthy", worker_state_schema["schema"]["properties"])
            worker_demand_schema = _run_cli("schema", "runtime-worker-demand")
            self.assertEqual(worker_demand_schema["model"], "runtime-worker-demand")
            self.assertIn("worker_deficit_count", worker_demand_schema["schema"]["properties"])
            self.assertIn("operation_type", worker_demand_schema["schema"]["properties"])
            capability_demand_schema = _run_cli("schema", "runtime-capability-demand")
            self.assertEqual(
                capability_demand_schema["model"],
                "runtime-capability-demand",
            )
            self.assertIn(
                "operation_type",
                capability_demand_schema["schema"]["properties"],
            )

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

    def test_package_registry_index_exposes_release_digests(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "pkg"
            package_dir.mkdir()
            package_yaml = package_dir / "process-runtime-package.yaml"
            pipeline_yaml = package_dir / "extract.yaml"
            package_yaml.write_text(
                textwrap.dedent(
                    """
                    package: documents_pkg
                    version: "2026.06"
                    title: Documents package
                    document_types:
                      - id: generic_document
                    operation_types:
                      - id: extract
                        category: extraction
                    artifact_kinds:
                      - id: extracted_payload
                    secrets:
                      - id: llm_api_key
                        env_var: LLM_API_KEY
                    capabilities:
                      - id: extract_document
                        operation_type: extract
                        accepts_document_types: [generic_document]
                        emits_artifact_kinds: [extracted_payload]
                    pipelines:
                      - extract.yaml
                    workers:
                      - id: extract_worker
                        pipeline: extract_flow
                        process: extract
                        capabilities: [extract_document]
                        secrets: [llm_api_key]
                        command: ["python", "extract.py"]
                    """
                ).strip(),
                encoding="utf-8",
            )
            pipeline_yaml.write_text(
                textwrap.dedent(
                    """
                    pipeline: extract_flow
                    version: "9"
                    steps:
                      - id: extract
                        capability: extract_document
                        adapter:
                          kind: queue
                          queue: documents.extract
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "run-input.example.yaml").write_text(
                textwrap.dedent(
                    """
                    run_id: run_extract
                    pipeline_id: extract_flow
                    documents:
                      - document_id: doc_1
                        document_type: generic_document
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "extract.py").write_text(
                "from fala.sdk import run_stdio\n"
                "\n"
                "run_stdio(lambda ctx: {'values': {'ok': True}})\n",
                encoding="utf-8",
            )
            registry = PipelineRegistry.from_directory(root)
            index = build_workflow_registry_index(registry)
            readiness = build_workflow_readiness_report(registry)

            self.assertEqual(index.package_count, 1)
            release = index.packages[0]
            self.assertEqual(release.package_id, "documents_pkg")
            self.assertEqual(release.version, "2026.06")
            self.assertEqual(release.pipeline_ids, ["extract_flow"])
            self.assertEqual(release.worker_ids, ["extract_worker"])
            self.assertEqual(release.secret_ids, ["llm_api_key"])
            self.assertEqual(release.operation_type_ids, ["extract"])
            self.assertEqual(release.source, str(package_yaml))
            self.assertEqual(
                release.manifest_file.sha256 if release.manifest_file else None,
                hashlib.sha256(package_yaml.read_bytes()).hexdigest(),
            )
            self.assertEqual(release.pipelines[0].version, "9")
            self.assertEqual(release.pipelines[0].source, str(pipeline_yaml.resolve()))
            self.assertRegex(release.contract_sha256, r"^[0-9a-f]{64}$")
            self.assertRegex(release.pipelines[0].contract_sha256, r"^[0-9a-f]{64}$")
            self.assertTrue(readiness.ok)
            self.assertEqual(readiness.package_count, 1)
            self.assertEqual(readiness.warning_count, 0)
            ready_package = readiness.packages[0]
            self.assertEqual(ready_package.package_id, "documents_pkg")
            self.assertEqual(ready_package.operation_type_ids, ["extract"])
            self.assertEqual(
                ready_package.routeable_document_type_ids,
                ["generic_document"],
            )
            self.assertEqual(ready_package.queue_process_count, 1)
            self.assertEqual(ready_package.covered_queue_process_count, 1)
            self.assertFalse(ready_package.missing_worker_process_ids)
            self.assertEqual(ready_package.invalid_worker_command_count, 0)
            self.assertEqual(ready_package.invalid_worker_ids, [])
            self.assertTrue(ready_package.sample_files["run_input_example"])

            cli = _run_cli("--pipeline-dir", str(root), "package-index")
            self.assertTrue(cli["ok"])
            self.assertEqual(
                cli["index"]["packages"][0]["contract_sha256"],
                release.contract_sha256,
            )
            self.assertEqual(
                cli["index"]["packages"][0]["operation_type_ids"],
                ["extract"],
            )
            output_path = root / "package-index.json"
            written = _run_cli(
                "--pipeline-dir",
                str(root),
                "package-index",
                "--output",
                str(output_path),
            )
            self.assertTrue(written["ok"])
            self.assertTrue(output_path.exists())
            doctor = _run_cli("--pipeline-dir", str(root), "package-doctor")
            self.assertTrue(doctor["ok"])
            self.assertEqual(doctor["readiness"]["package_count"], 1)
            self.assertEqual(
                doctor["readiness"]["packages"][0]["covered_queue_process_count"],
                1,
            )
            self.assertEqual(
                doctor["readiness"]["packages"][0]["operation_type_ids"],
                ["extract"],
            )
            self.assertEqual(
                doctor["readiness"]["packages"][0]["invalid_worker_command_count"],
                0,
            )
            doctor_output = root / "package-readiness.json"
            doctor_written = _run_cli(
                "--pipeline-dir",
                str(root),
                "package-doctor",
                "--output",
                str(doctor_output),
            )
            self.assertTrue(doctor_written["ok"])
            self.assertTrue(doctor_output.exists())

            app = FastAPI()
            service = RuntimeService(registry=registry, store=InMemoryStateStore())
            app.include_router(create_runtime_router(service), prefix="/api")

            async def exercise_api() -> None:
                async with ProcessRuntimeClient(
                    "http://runtime.test",
                    transport=httpx.ASGITransport(app=app),
                ) as client:
                    api_index = await client.get_package_index()
                    self.assertEqual(api_index["package_count"], 1)
                    api_release = await client.get_package_release("documents_pkg")
                    self.assertEqual(
                        api_release["contract_sha256"],
                        release.contract_sha256,
                    )
                    api_readiness = await client.get_package_readiness()
                    self.assertTrue(api_readiness["ok"])
                    self.assertEqual(api_readiness["package_count"], 1)
                    api_package_readiness = await client.get_package_readiness(
                        package_id="documents_pkg",
                    )
                    self.assertTrue(api_package_readiness["ok"])
                    self.assertEqual(
                        api_package_readiness["package_id"],
                        "documents_pkg",
                    )

            asyncio.run(exercise_api())

    def test_package_doctor_reports_bootstrap_gaps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "pkg"
            package_dir.mkdir()
            (package_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: incomplete_documents
                    document_types:
                      - id: generic_document
                      - id: email_document
                    artifact_kinds:
                      - id: extracted_payload
                    capabilities:
                      - id: extract_document
                        accepts_document_types: [generic_document]
                        emits_document_types: [email_document]
                        emits_artifact_kinds: [extracted_payload]
                      - id: summarize_document
                    pipelines:
                      - extract.yaml
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "extract.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: extract_flow
                    steps:
                      - id: extract
                        capability: extract_document
                        adapter:
                          kind: queue
                          queue: documents.extract
                    """
                ).strip(),
                encoding="utf-8",
            )

            report = build_workflow_readiness_report(
                PipelineRegistry.from_directory(root)
            )

            self.assertTrue(report.ok)
            self.assertEqual(report.package_count, 1)
            package = report.packages[0]
            self.assertEqual(package.package_id, "incomplete_documents")
            self.assertEqual(package.routeable_document_type_ids, ["generic_document"])
            self.assertEqual(package.unrouteable_document_type_ids, ["email_document"])
            self.assertEqual(package.emitted_document_type_ids, ["email_document"])
            self.assertEqual(package.queue_process_count, 1)
            self.assertEqual(package.covered_queue_process_count, 0)
            self.assertEqual(package.missing_worker_process_ids, ["extract_flow/extract"])
            codes = {issue.code for issue in package.issues}
            self.assertIn("queue_step_without_package_worker", codes)
            self.assertIn("document_type_not_routeable", codes)
            self.assertIn("capability_unused", codes)
            self.assertIn("sample_run_input_missing", codes)

    def test_package_doctor_reports_backpressure_stream_without_consumers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "pkg"
            package_dir.mkdir()
            (package_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: stream_documents
                    document_types:
                      - id: generic_document
                    capabilities:
                      - id: extract_pages
                        accepts_document_types: [generic_document]
                        emits_streams:
                          - stream: pages
                            kinds: [page]
                            max_buffered_chunks: 8
                    pipelines:
                      - stream.yaml
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "stream.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: stream_flow
                    steps:
                      - id: extract
                        capability: extract_pages
                        adapter:
                          kind: queue
                          queue: stream.extract
                    """
                ).strip(),
                encoding="utf-8",
            )

            report = build_workflow_readiness_report(
                PipelineRegistry.from_directory(root)
            )

            package = report.packages[0]
            codes = {issue.code for issue in package.issues}
            self.assertIn("stream_backpressure_without_declared_consumers", codes)

    def test_package_doctor_reports_dag_artifact_contract_gaps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "pkg"
            package_dir.mkdir()
            (package_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: dag_documents
                    document_types:
                      - id: generic_document
                    artifact_kinds:
                      - id: source_payload
                      - id: review_payload
                      - id: exported_payload
                    capabilities:
                      - id: extract_document
                        accepts_document_types: [generic_document]
                        emits_artifact_kinds: [source_payload]
                      - id: review_document
                        accepts_artifact_kinds: [source_payload]
                        emits_artifact_kinds: [review_payload]
                      - id: export_document
                        accepts_artifact_kinds: [source_payload]
                        emits_artifact_kinds: [exported_payload]
                    pipelines:
                      - dag.yaml
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "dag.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: dag_flow
                    steps:
                      - id: extract
                        capability: extract_document
                        adapter:
                          kind: manual
                      - id: review
                        needs: [extract]
                        capability: review_document
                        adapter:
                          kind: manual
                      - id: export
                        needs: [extract, review]
                        capability: export_document
                        adapter:
                          kind: manual
                    """
                ).strip(),
                encoding="utf-8",
            )

            report = build_workflow_readiness_report(
                PipelineRegistry.from_directory(root)
            )

            package = report.packages[0]
            codes = {issue.code for issue in package.issues}
            self.assertIn("capability_missing_needed_artifact_kinds", codes)
            issue = next(
                item
                for item in package.issues
                if item.code == "capability_missing_needed_artifact_kinds"
            )
            self.assertEqual(issue.pipeline_id, "dag_flow")
            self.assertEqual(issue.process_id, "export")
            self.assertEqual(issue.capability_id, "export_document")
            self.assertEqual(issue.data["missing_artifact_kinds"], ["review_payload"])

    def test_package_doctor_reports_unavailable_worker_commands(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "pkg"
            package_dir.mkdir()
            (package_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: worker_documents
                    document_types:
                      - id: generic_document
                    artifact_kinds:
                      - id: extracted_payload
                    capabilities:
                      - id: extract_document
                        accepts_document_types: [generic_document]
                        emits_artifact_kinds: [extracted_payload]
                    pipelines:
                      - extract.yaml
                    workers:
                      - id: extract_worker
                        pipeline: extract_flow
                        process: extract
                        capabilities: [extract_document]
                        command: ["python", "steps/missing.py"]
                        cwd: "."
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "extract.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: extract_flow
                    steps:
                      - id: extract
                        capability: extract_document
                        adapter:
                          kind: queue
                          queue: documents.extract
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "run-input.example.yaml").write_text(
                textwrap.dedent(
                    """
                    run_id: run_extract
                    pipeline_id: extract_flow
                    documents:
                      - document_id: doc_1
                        document_type: generic_document
                    """
                ).strip(),
                encoding="utf-8",
            )

            report = build_workflow_readiness_report(
                PipelineRegistry.from_directory(root)
            )

            package = report.packages[0]
            self.assertTrue(report.ok)
            self.assertEqual(package.queue_process_count, 1)
            self.assertEqual(package.covered_queue_process_count, 1)
            self.assertEqual(package.invalid_worker_command_count, 1)
            self.assertEqual(package.invalid_worker_ids, ["extract_worker"])
            issues = {issue.code: issue for issue in package.issues}
            self.assertIn("worker_command_unavailable", issues)
            self.assertEqual(
                issues["worker_command_unavailable"].worker_id,
                "extract_worker",
            )
            self.assertIn(
                "command file path does not exist",
                issues["worker_command_unavailable"].data["reason"],
            )

            doctor = _run_cli("--pipeline-dir", str(root), "package-doctor")
            self.assertEqual(
                doctor["readiness"]["packages"][0]["invalid_worker_ids"],
                ["extract_worker"],
            )

    def test_package_doctor_reports_missing_source_list_sample_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "pkg"
            package_dir.mkdir()
            (package_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: sample_documents
                    document_types:
                      - id: generic_document
                    artifact_kinds:
                      - id: extracted_payload
                    capabilities:
                      - id: extract_document
                        accepts_document_types: [generic_document]
                        emits_artifact_kinds: [extracted_payload]
                    pipelines:
                      - extract.yaml
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "extract.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: extract_flow
                    steps:
                      - id: extract
                        capability: extract_document
                        adapter:
                          kind: subprocess
                          command: ["python", "steps/extract.py"]
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "run-input.example.yaml").write_text(
                textwrap.dedent(
                    """
                    run_id: run_sample
                    pipeline_id: extract_flow
                    documents:
                      - document_id: sample.txt
                        document_type: generic_document
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "source-list.example.csv").write_text(
                "document_id,path,document_type\n"
                "sample.txt,incoming/missing.txt,generic_document\n",
                encoding="utf-8",
            )

            report = build_workflow_readiness_report(
                PipelineRegistry.from_directory(root)
            )

            package = report.packages[0]
            self.assertTrue(report.ok)
            self.assertFalse(
                package.sample_files["source_list_local_sources_present"]
            )
            self.assertTrue(package.sample_files["run_input_example_valid"])
            self.assertTrue(package.sample_files["source_list_example_valid"])
            codes = {issue.code for issue in package.issues}
            self.assertIn("sample_source_files_missing", codes)

    def test_package_doctor_reports_invalid_sample_manifests(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "pkg"
            package_dir.mkdir()
            incoming = package_dir / "incoming"
            incoming.mkdir()
            (incoming / "sample.txt").write_text("sample\n", encoding="utf-8")
            (package_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: sample_documents
                    document_types:
                      - id: generic_document
                    artifact_kinds:
                      - id: extracted_payload
                    capabilities:
                      - id: extract_document
                        accepts_document_types: [generic_document]
                        emits_artifact_kinds: [extracted_payload]
                    pipelines:
                      - extract.yaml
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "extract.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: extract_flow
                    steps:
                      - id: extract
                        capability: extract_document
                        adapter:
                          kind: subprocess
                          command: ["python", "steps/extract.py"]
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "run-input.example.yaml").write_text(
                textwrap.dedent(
                    """
                    run_id: run_sample
                    pipeline_id: extract_flow
                    documents:
                      - document_id: sample.txt
                        document_type: unknown_document
                        source_uri: incoming/sample.txt
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "source-list.example.csv").write_text(
                "document_id,path,document_type\n"
                "sample.txt,incoming/sample.txt,unknown_document\n",
                encoding="utf-8",
            )

            report = build_workflow_readiness_report(
                PipelineRegistry.from_directory(root)
            )

            package = report.packages[0]
            self.assertTrue(report.ok)
            self.assertFalse(package.sample_files["run_input_example_valid"])
            self.assertFalse(package.sample_files["source_list_example_valid"])
            self.assertTrue(package.sample_files["source_list_local_sources_present"])
            codes = {issue.code for issue in package.issues}
            self.assertIn("sample_run_input_invalid", codes)
            self.assertIn("sample_source_list_invalid", codes)

    def test_basic_example_package_has_bootstrap_manifests(self) -> None:
        doctor = _run_cli(
            "--pipeline-dir",
            "examples/pipelines",
            "package-doctor",
        )
        self.assertTrue(doctor["ok"])
        self.assertEqual(doctor["readiness"]["warning_count"], 0)
        package = doctor["readiness"]["packages"][0]
        self.assertTrue(package["sample_files"]["run_input_example"])
        self.assertTrue(package["sample_files"]["run_input_example_valid"])
        self.assertTrue(package["sample_files"]["source_list_example"])
        self.assertTrue(package["sample_files"]["source_list_example_valid"])
        self.assertTrue(package["sample_files"]["source_list_local_sources_present"])
        self.assertTrue(package["sample_files"]["makefile"])

        validated = _run_cli(
            "--pipeline-dir",
            "examples/pipelines",
            "validate-run",
            "--run-input",
            "examples/pipelines/basic/run-input.example.yaml",
        )
        self.assertTrue(validated["ok"])
        self.assertEqual(validated["document_count"], 1)

        discovered = _run_cli(
            "discover-documents",
            "--source-list",
            "examples/pipelines/basic/source-list.example.csv",
            "--pipeline",
            "basic_enrichment",
            "--run-id",
            "run_basic_from_csv",
        )
        self.assertEqual(discovered["run_id"], "run_basic_from_csv")
        self.assertEqual(discovered["documents"][0]["document_id"], "sample.txt")
        self.assertTrue(discovered["documents"][0]["source_uri"].startswith("file://"))

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

    def test_runtime_api_and_client_expose_pipeline_contract(self) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="pkg",
                pipelines=["typed.yaml"],
                document_types=[
                    DocumentTypeSpec(
                        id="generic_document",
                        media_types=["text/plain"],
                    )
                ],
                artifact_kinds=[
                    ArtifactKindSpec(
                        id="extracted_text",
                        media_types=["text/plain"],
                    )
                ],
                capabilities=[
                    CapabilitySpec(
                        id="extract_text",
                        accepts_document_types=["generic_document"],
                        emits_artifact_kinds=["extracted_text"],
                        output_schema={
                            "type": "object",
                            "required": ["text"],
                            "properties": {"text": {"type": "string"}},
                        },
                    )
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="typed_flow",
                steps=[
                    ProcessSpec(
                        id="extract",
                        capability="extract_text",
                        priority=20,
                        max_concurrency=3,
                        adapter=AdapterSpec(kind="queue", queue="typed.extract"),
                    )
                ],
            ),
            package_id="pkg",
        )
        registry.validate_package_workers("pkg")
        service = RuntimeService(
            registry=registry,
            store=InMemoryStateStore(),
        )
        app = FastAPI()
        app.include_router(create_runtime_router(service), prefix="/api")

        async def run_client() -> None:
            async with ProcessRuntimeClient(
                "http://runtime.test",
                transport=httpx.ASGITransport(app=app),
            ) as client:
                contract = await client.get_pipeline_contract("typed_flow")
                self.assertEqual(contract["pipeline_id"], "typed_flow")
                self.assertEqual(contract["package_id"], "pkg")
                self.assertEqual(contract["document_types"][0]["id"], "generic_document")
                self.assertEqual(contract["artifact_kinds"][0]["id"], "extracted_text")
                self.assertEqual(contract["steps"][0]["capability"]["id"], "extract_text")
                self.assertEqual(
                    contract["steps"][0]["emitted_artifact_kinds"][0]["id"],
                    "extracted_text",
                )
                self.assertEqual(
                    contract["steps"][0]["output_schema"]["required"],
                    ["text"],
                )
                self.assertEqual(contract["steps"][0]["priority"], 20)
                self.assertEqual(contract["steps"][0]["max_concurrency"], 3)

        asyncio.run(run_client())

    def test_runtime_api_and_client_expose_scaffold_blueprints(self) -> None:
        service = RuntimeService(
            registry=PipelineRegistry([]),
            store=InMemoryStateStore(),
        )
        app = FastAPI()
        app.include_router(create_runtime_router(service), prefix="/api")

        async def run_client() -> None:
            transport = httpx.ASGITransport(app=app)
            async with ProcessRuntimeClient(
                "http://runtime.test",
                transport=transport,
            ) as client:
                catalog = await client.list_blueprints()
                self.assertTrue(catalog["ok"])
                self.assertEqual(catalog["blueprint_count"], 10)
                filtered = await client.list_blueprints(query="redacted_document")
                self.assertEqual(filtered["query"], "redacted_document")
                self.assertEqual(
                    [item["id"] for item in filtered["blueprints"]],
                    ["document_redaction_review"],
                )
                self.assertEqual(
                    [item["id"] for item in catalog["blueprints"]],
                    [
                        "document_digitalization",
                        "email_processing",
                        "document_package_processing",
                        "document_redaction_review",
                        "document_translation_review",
                        "generative_media",
                        "llm_document_processing",
                        "knowledge_base_ingestion",
                        "structured_extraction_review",
                        "tabular_data_processing",
                    ],
                )
                selected = await client.get_blueprint("llm_document_processing")
                self.assertEqual(selected["document"]["type"], "llm_document")
                self.assertEqual(selected["manual_steps"], ["review"])
                self.assertIn("llm_pool", selected["resource_pools"])
                steps = {step["id"]: step for step in selected["steps"]}
                self.assertEqual(steps["generate"]["capability"], "generate_response")
                self.assertEqual(steps["generate"]["streams"][0]["stream_id"], "tokens")

            async with httpx.AsyncClient(
                transport=transport,
                base_url="http://runtime.test",
            ) as raw:
                missing = await raw.get("/api/process-runtime/blueprints/not_real")
                self.assertEqual(missing.status_code, 404)

        asyncio.run(run_client())

    def test_runtime_route_report_explains_rejected_candidates(self) -> None:
        routed, report = route_runtime_documents_with_report(
            [
                RuntimeDocumentInput(
                    document_id="contract.txt",
                    media_type="text/plain",
                    source_uri="file:///tmp/contract.txt",
                    metadata={"tenant": "acme"},
                    values={"kind": "letter"},
                )
            ],
            routes=[
                {
                    "id": "invoices",
                    "match": {
                        "extensions": [".pdf"],
                        "metadata": {"tenant": "billing"},
                    },
                    "set": {
                        "pipeline_id": "invoice_flow",
                        "document_type": "invoice_document",
                    },
                },
                {
                    "id": "emails",
                    "match": {"media_types": ["message/*"]},
                    "set": {
                        "pipeline_id": "email_flow",
                        "document_type": "email_document",
                    },
                },
            ],
        )

        self.assertIsNone(routed[0].pipeline_id)
        self.assertIsNone(routed[0].document_type)
        self.assertEqual(report["document_count"], 1)
        self.assertEqual(report["routed_count"], 0)
        self.assertEqual(report["unrouted_count"], 1)
        self.assertEqual(report["candidate_count"], 2)
        self.assertEqual(report["matched_candidate_count"], 0)
        document_report = report["documents"][0]
        self.assertFalse(document_report["changed"])
        self.assertEqual(document_report["candidate_count"], 2)
        self.assertEqual(document_report["matched_candidate_count"], 0)
        invoice_candidate = next(
            item for item in document_report["candidates"] if item["route_id"] == "invoices"
        )
        self.assertFalse(invoice_candidate["match"])
        self.assertEqual(
            {reason["field"] for reason in invoice_candidate["reasons"]},
            {"extension", "metadata.tenant"},
        )
        email_candidate = next(
            item for item in document_report["candidates"] if item["route_id"] == "emails"
        )
        self.assertFalse(email_candidate["match"])
        self.assertEqual(
            email_candidate["reasons"][0]["reason"],
            "media_type_mismatch",
        )
        self.assertEqual(
            [item["route_id"] for item in document_report["unmatched_reasons"]],
            ["invoices", "emails"],
        )

    def test_runtime_api_and_client_validate_run_input_without_creating_run(self) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="pkg",
                pipelines=["case.yaml"],
                document_types=[
                    DocumentTypeSpec(
                        id="case_document",
                        media_types=["application/pdf"],
                        extensions=[".pdf"],
                        metadata_schema={
                            "type": "object",
                            "required": ["case_id"],
                            "properties": {"case_id": {"type": "string"}},
                            "additionalProperties": False,
                        },
                    )
                ],
                capabilities=[
                    CapabilitySpec(
                        id="ingest_case",
                        accepts_document_types=["case_document"],
                    )
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="case_flow",
                steps=[
                    ProcessSpec(
                        id="ingest",
                        capability="ingest_case",
                        adapter=AdapterSpec(kind="queue", queue="case.ingest"),
                    )
                ],
            ),
            package_id="pkg",
        )
        registry.validate_package_workers("pkg")
        store = InMemoryStateStore()
        service = RuntimeService(
            registry=registry,
            store=store,
        )
        app = FastAPI()
        app.include_router(create_runtime_router(service), prefix="/api")

        async def run_client() -> None:
            transport = httpx.ASGITransport(app=app)
            async with ProcessRuntimeClient(
                "http://runtime.test",
                transport=transport,
            ) as client:
                preview = await client.validate_run(
                    run_id="run_preview",
                    pipeline_id="case_flow",
                    documents=[
                        RuntimeDocumentInput(
                            document_id="case.pdf",
                            document_type="case_document",
                            media_type="application/pdf",
                            source_uri="file:///tmp/case.pdf",
                            metadata={"case_id": "C-1"},
                        )
                    ],
                )
                self.assertTrue(preview["ok"])
                self.assertEqual(preview["document_count"], 1)
                self.assertEqual(preview["documents"][0]["metadata_keys"], ["case_id"])
                self.assertEqual(
                    preview["contracts"]["case_flow"]["document_types"][0]["id"],
                    "case_document",
                )
                self.assertIsNone(await store.get_run("run_preview"))
                self.assertEqual(await store.list_documents(run_id="run_preview"), [])

            async with httpx.AsyncClient(
                transport=transport,
                base_url="http://runtime.test",
            ) as raw_client:
                invalid = await raw_client.post(
                    "/api/process-runtime/runs/validate",
                    json={
                        "run_id": "run_invalid_preview",
                        "pipeline_id": "case_flow",
                        "documents": [
                            {
                                "document_id": "case.pdf",
                                "document_type": "case_document",
                                "media_type": "application/pdf",
                                "source_uri": "file:///tmp/case.pdf",
                                "metadata": {"mailbox": "ops"},
                            }
                        ],
                    },
                )
                self.assertEqual(invalid.status_code, 400, invalid.text)
                self.assertIn("'case_id' is a required property", invalid.json()["detail"])
                self.assertIsNone(await store.get_run("run_invalid_preview"))
                self.assertEqual(await store.list_documents(run_id="run_invalid_preview"), [])

        asyncio.run(run_client())

    def test_runtime_api_and_client_auto_route_run_input_from_contracts(self) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="mixed_pkg",
                pipelines=["invoice.yaml", "email.yaml"],
                document_types=[
                    DocumentTypeSpec(
                        id="invoice_document",
                        media_types=["application/pdf"],
                        extensions=[".pdf"],
                    ),
                    DocumentTypeSpec(
                        id="email_document",
                        media_types=["message/rfc822"],
                        extensions=[".eml"],
                    ),
                ],
                artifact_kinds=[
                    ArtifactKindSpec(id="invoice_source"),
                    ArtifactKindSpec(id="email_source"),
                ],
                capabilities=[
                    CapabilitySpec(
                        id="ingest_invoice",
                        accepts_document_types=["invoice_document"],
                        emits_artifact_kinds=["invoice_source"],
                    ),
                    CapabilitySpec(
                        id="ingest_email",
                        accepts_document_types=["email_document"],
                        emits_artifact_kinds=["email_source"],
                    ),
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="invoice_flow",
                steps=[
                    ProcessSpec(
                        id="ingest_invoice",
                        capability="ingest_invoice",
                        adapter=AdapterSpec(kind="queue", queue="invoice.ingest"),
                    )
                ],
            ),
            package_id="mixed_pkg",
        )
        registry.add(
            PipelineSpec(
                id="email_flow",
                steps=[
                    ProcessSpec(
                        id="ingest_email",
                        capability="ingest_email",
                        adapter=AdapterSpec(kind="queue", queue="email.ingest"),
                    )
                ],
            ),
            package_id="mixed_pkg",
        )
        registry.validate_package_workers("mixed_pkg")
        store = InMemoryStateStore()
        service = RuntimeService(registry=registry, store=store)
        app = FastAPI()
        app.include_router(create_runtime_router(service), prefix="/api")

        async def run_client() -> None:
            transport = httpx.ASGITransport(app=app)
            async with ProcessRuntimeClient(
                "http://runtime.test",
                transport=transport,
            ) as client:
                route = await client.route_run(
                    run_id="run_api_route",
                    auto_route=True,
                    documents=[
                        RuntimeDocumentInput(
                            document_id="invoice.pdf",
                            media_type="application/pdf",
                            source_uri="file:///tmp/invoice.pdf",
                        ),
                        RuntimeDocumentInput(
                            document_id="mail.eml",
                            media_type="message/rfc822",
                            source_uri="file:///tmp/mail.eml",
                        ),
                    ],
                )
                self.assertEqual(
                    {
                        document["document_id"]: (
                            document["pipeline_id"],
                            document["document_type"],
                        )
                        for document in route["run_input"]["documents"]
                    },
                    {
                        "invoice.pdf": ("invoice_flow", "invoice_document"),
                        "mail.eml": ("email_flow", "email_document"),
                    },
                )
                self.assertEqual(route["route_report"]["document_count"], 2)
                self.assertEqual(route["route_report"]["routed_count"], 2)
                self.assertEqual(route["route_report"]["unrouted_count"], 0)
                self.assertEqual(route["route_report"]["candidate_count"], 4)
                self.assertEqual(route["route_report"]["matched_candidate_count"], 2)
                invoice_report = next(
                    item
                    for item in route["route_report"]["documents"]
                    if item["document_id"] == "invoice.pdf"
                )
                self.assertTrue(invoice_report["changed"])
                self.assertEqual(invoice_report["candidate_count"], 2)
                self.assertEqual(invoice_report["matched_candidate_count"], 1)
                self.assertEqual(invoice_report["original"]["metadata_keys"], [])
                self.assertEqual(invoice_report["routed"]["metadata_keys"], [])
                self.assertEqual(invoice_report["routes"][0]["kind"], "auto")
                self.assertEqual(
                    invoice_report["routes"][0]["route_id"],
                    "auto:invoice_flow:invoice_document",
                )
                invoice_candidate = next(
                    item
                    for item in invoice_report["candidates"]
                    if item["route_id"] == "auto:invoice_flow:invoice_document"
                )
                self.assertTrue(invoice_candidate["match"])
                email_candidate = next(
                    item
                    for item in invoice_report["candidates"]
                    if item["route_id"] == "auto:email_flow:email_document"
                )
                self.assertFalse(email_candidate["match"])
                self.assertIn(
                    "extension",
                    {reason["field"] for reason in email_candidate["reasons"]},
                )
                self.assertIn(
                    "media_type",
                    {reason["field"] for reason in email_candidate["reasons"]},
                )
                self.assertEqual(
                    invoice_report["unmatched_reasons"][0]["route_id"],
                    "auto:email_flow:email_document",
                )
                self.assertIn(
                    "extension",
                    {
                        evidence["field"]
                        for evidence in invoice_report["routes"][0]["evidence"]
                    },
                )
                routed_invoice = route["run_input"]["documents"][0]
                self.assertEqual(routed_invoice["metadata"], {})
                self.assertNotIn("route_report", routed_invoice["metadata"])

                preview = await client.validate_run(
                    run_id="run_api_preview",
                    auto_route=True,
                    documents=[
                        {
                            "document_id": "invoice.pdf",
                            "media_type": "application/pdf",
                            "source_uri": "file:///tmp/invoice.pdf",
                        }
                    ],
                )
                self.assertEqual(preview["documents"][0]["pipeline_id"], "invoice_flow")
                self.assertEqual(
                    preview["documents"][0]["document_type"],
                    "invoice_document",
                )
                self.assertIsNone(await store.get_run("run_api_preview"))

                created = await client.create_run(
                    run_id="run_api_auto",
                    auto_route=True,
                    documents=[
                        {
                            "document_id": "mail.eml",
                            "media_type": "message/rfc822",
                            "source_uri": "file:///tmp/mail.eml",
                        }
                    ],
                )
                self.assertEqual(created["run"]["id"], "run_api_auto")
                self.assertEqual(created["document_count"], 1)
                self.assertEqual(created["schedules"][0]["pipeline_id"], "email_flow")
                created_provenance = created["run"]["metadata"]["process_runtime"][
                    "run_provenance"
                ]
                self.assertEqual(
                    created_provenance["route_report"]["documents"][0]["routes"][0][
                        "route_id"
                    ],
                    "auto:email_flow:email_document",
                )
                self.assertEqual(
                    len(created_provenance["route_report_sha256"]),
                    64,
                )
                provenance = await client.run_provenance(run_id="run_api_auto")
                self.assertTrue(provenance["has_provenance"])
                self.assertEqual(
                    provenance["provenance"]["route_report"]["documents"][0][
                        "routes"
                    ][0]["route_id"],
                    "auto:email_flow:email_document",
                )
                self.assertEqual(
                    len(provenance["provenance"]["run_input_sha256"]),
                    64,
                )
                document = await store.get_document(
                    run_id="run_api_auto",
                    document_id="mail.eml",
                )
                self.assertIsNotNone(document)
                assert document is not None
                self.assertEqual(document.pipeline_id, "email_flow")
                self.assertEqual(document.document_type, "email_document")

                appended = await client.append_documents(
                    run_id="run_api_auto",
                    auto_route=True,
                    documents=[
                        {
                            "document_id": "invoice.pdf",
                            "media_type": "application/pdf",
                            "source_uri": "file:///tmp/invoice.pdf",
                        }
                    ],
                )
                self.assertEqual(appended["document_count"], 1)
                self.assertEqual(appended["route_report"]["document_count"], 1)
                self.assertEqual(appended["route_report"]["routed_count"], 1)
                self.assertEqual(
                    appended["route_report"]["documents"][0]["routes"][0]["route_id"],
                    "auto:invoice_flow:invoice_document",
                )
                provenance_after_append = await client.run_provenance(
                    run_id="run_api_auto"
                )
                append_batches = provenance_after_append["provenance"][
                    "append_batches"
                ]
                self.assertEqual(len(append_batches), 1)
                self.assertEqual(append_batches[0]["batch_id"], "append-0001")
                self.assertEqual(append_batches[0]["document_ids"], ["invoice.pdf"])
                self.assertEqual(append_batches[0]["route_report"]["routed_count"], 1)
                self.assertEqual(
                    append_batches[0]["route_report"]["documents"][0]["routes"][0][
                        "route_id"
                    ],
                    "auto:invoice_flow:invoice_document",
                )
                invoice = await store.get_document(
                    run_id="run_api_auto",
                    document_id="invoice.pdf",
                )
                self.assertIsNotNone(invoice)
                assert invoice is not None
                self.assertEqual(invoice.pipeline_id, "invoice_flow")
                self.assertEqual(invoice.document_type, "invoice_document")

        asyncio.run(run_client())

    def test_runtime_api_and_client_plan_run_input_without_creating_run(self) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="pkg",
                pipelines=["case.yaml"],
                document_types=[
                    DocumentTypeSpec(
                        id="case_document",
                        media_types=["application/pdf"],
                        extensions=[".pdf"],
                        metadata_schema={
                            "type": "object",
                            "required": ["case_id"],
                            "properties": {"case_id": {"type": "string"}},
                            "additionalProperties": False,
                        },
                    )
                ],
                artifact_kinds=[
                    ArtifactKindSpec(id="case_payload"),
                    ArtifactKindSpec(id="classified_case"),
                ],
                capabilities=[
                    CapabilitySpec(
                        id="ingest_case",
                        accepts_document_types=["case_document"],
                        emits_artifact_kinds=["case_payload"],
                    ),
                    CapabilitySpec(
                        id="classify_case",
                        accepts_artifact_kinds=["case_payload"],
                        emits_artifact_kinds=["classified_case"],
                    ),
                ],
                workers=[
                    WorkflowWorkerSpec(
                        id="ingest_worker",
                        capabilities=["ingest_case"],
                        pipeline_id="case_flow",
                        process_id="ingest",
                        command=["python", "workers/ingest.py"],
                        resources=ResourceSpec(gpu_count=1, units={"ocr_slots": 1}),
                    ),
                    WorkflowWorkerSpec(
                        id="classify_worker",
                        capabilities=["classify_case"],
                        pipeline_id="case_flow",
                        process_id="classify",
                        command=["python", "workers/classify.py"],
                    ),
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="case_flow",
                steps=[
                    ProcessSpec(
                        id="ingest",
                        capability="ingest_case",
                        max_concurrency=2,
                        resource_pool="gpu",
                        resources=ResourceSpec(gpu_count=1, units={"ocr_slots": 1}),
                        adapter=AdapterSpec(kind="queue", queue="case.ingest"),
                    ),
                    ProcessSpec(
                        id="classify",
                        capability="classify_case",
                        needs=["ingest"],
                        max_concurrency=1,
                        adapter=AdapterSpec(kind="queue", queue="case.classify"),
                    ),
                ],
            ),
            package_id="pkg",
        )
        registry.validate_package_workers("pkg")
        store = InMemoryStateStore()
        service = RuntimeService(registry=registry, store=store)
        app = FastAPI()
        app.include_router(create_runtime_router(service), prefix="/api")

        async def run_client() -> None:
            transport = httpx.ASGITransport(app=app)
            async with ProcessRuntimeClient(
                "http://runtime.test",
                transport=transport,
            ) as client:
                plan = await client.plan_run(
                    run_id="run_plan",
                    pipeline_id="case_flow",
                    config={
                        "resource_pools": {
                            "gpu": {"gpu_count": 1, "units": {"ocr_slots": 1}}
                        }
                    },
                    documents=[
                        RuntimeDocumentInput(
                            document_id="a.pdf",
                            document_type="case_document",
                            media_type="application/pdf",
                            source_uri="file:///tmp/a.pdf",
                            metadata={"case_id": "A"},
                        ),
                        RuntimeDocumentInput(
                            document_id="b.pdf",
                            document_type="case_document",
                            media_type="application/pdf",
                            source_uri="file:///tmp/b.pdf",
                            metadata={"case_id": "B"},
                        ),
                    ],
                )
                self.assertTrue(plan["ok"])
                self.assertEqual(plan["document_count"], 2)
                self.assertEqual(plan["plan"]["process_instance_count"], 4)
                self.assertEqual(plan["plan"]["queued_count"], 2)
                self.assertEqual(plan["plan"]["waiting_count"], 2)
                self.assertEqual(plan["plan"]["worker_demand_count"], 2)

                ingest = next(
                    item
                    for item in plan["plan"]["processes"]
                    if item["process_id"] == "ingest"
                )
                self.assertEqual(ingest["queued_count"], 2)
                self.assertEqual(ingest["waiting_count"], 0)
                self.assertEqual(ingest["initial_target_worker_count"], 2)
                self.assertEqual(ingest["eventual_target_worker_count"], 2)
                self.assertEqual(ingest["declared_worker_ids"], ["ingest_worker"])
                self.assertFalse(ingest["missing_declared_worker"])
                self.assertEqual(ingest["queued_resource_total"]["gpu_count"], 2)
                self.assertEqual(
                    ingest["queued_resource_total"]["units"],
                    {"ocr_slots": 2.0},
                )

                classify = next(
                    item
                    for item in plan["plan"]["processes"]
                    if item["process_id"] == "classify"
                )
                self.assertEqual(classify["queued_count"], 0)
                self.assertEqual(classify["waiting_count"], 2)
                self.assertEqual(classify["initial_target_worker_count"], 0)
                self.assertEqual(classify["eventual_target_worker_count"], 1)
                self.assertEqual(classify["declared_worker_ids"], ["classify_worker"])

                gpu_pool = next(
                    item for item in plan["plan"]["resource_pools"] if item["id"] == "gpu"
                )
                self.assertEqual(gpu_pool["limit"]["gpu_count"], 1)
                self.assertEqual(gpu_pool["queued_count"], 2)
                self.assertEqual(gpu_pool["total_resource_request"]["gpu_count"], 2)
                self.assertEqual(
                    plan["plan"]["documents"][0]["queued"][0]["process_id"],
                    "ingest",
                )
                self.assertIsNone(await store.get_run("run_plan"))
                self.assertEqual(await store.list_documents(run_id="run_plan"), [])

            async with httpx.AsyncClient(
                transport=transport,
                base_url="http://runtime.test",
            ) as raw_client:
                invalid = await raw_client.post(
                    "/api/process-runtime/runs/plan",
                    json={
                        "run_id": "run_invalid_plan",
                        "pipeline_id": "case_flow",
                        "documents": [
                            {
                                "document_id": "case.pdf",
                                "document_type": "case_document",
                                "media_type": "application/pdf",
                                "source_uri": "file:///tmp/case.pdf",
                                "metadata": {"mailbox": "ops"},
                            }
                        ],
                    },
                )
                self.assertEqual(invalid.status_code, 400, invalid.text)
                self.assertIn("'case_id' is a required property", invalid.json()["detail"])
                self.assertIsNone(await store.get_run("run_invalid_plan"))
                self.assertEqual(await store.list_documents(run_id="run_invalid_plan"), [])

        asyncio.run(run_client())

    def test_runtime_api_and_client_expose_queue_metrics(self) -> None:
        pipeline = PipelineSpec(
            id="metrics_flow",
            steps=[
                ProcessSpec(
                    id="ocr",
                    priority=30,
                    max_concurrency=1,
                    adapter=AdapterSpec(kind="queue", queue="metrics.ocr"),
                ),
                ProcessSpec(
                    id="export",
                    needs=["ocr"],
                    adapter=AdapterSpec(kind="queue", queue="metrics.export"),
                ),
            ],
        )
        store = InMemoryStateStore()
        service = RuntimeService(
            registry=PipelineRegistry([pipeline]),
            store=store,
        )
        app = FastAPI()
        app.include_router(create_runtime_router(service), prefix="/api")

        async def run_client() -> None:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_metrics",
                    pipeline_id="metrics_flow",
                    documents=[
                        RuntimeDocumentInput(document_id="doc_a"),
                        RuntimeDocumentInput(document_id="doc_b"),
                    ],
                )
            )
            claim = await service.claim_next(
                run_id="run_metrics",
                pipeline_id="metrics_flow",
                worker_id="worker-ocr",
                adapter_kind="queue",
            )
            self.assertIsNotNone(claim)
            assert claim is not None
            self.assertEqual(claim.document_id, "doc_a")

            direct = await service.queue_metrics("run_metrics")
            self.assertEqual(direct.document_count, 2)
            self.assertEqual(direct.queued_count, 1)
            self.assertEqual(direct.running_count, 1)
            self.assertEqual(direct.missing_worker_count, 1)
            ocr = next(item for item in direct.processes if item.process_id == "ocr")
            self.assertEqual(ocr.priority, 30)
            self.assertEqual(ocr.max_concurrency, 1)
            self.assertEqual(ocr.queued_count, 1)
            self.assertEqual(ocr.running_count, 1)
            self.assertEqual(ocr.missing_worker_count, 1)
            self.assertTrue(ocr.missing_worker)
            self.assertEqual(ocr.capacity_remaining, 0)
            self.assertTrue(ocr.saturated)
            self.assertEqual(ocr.oldest_queued_document_id, "doc_b")
            self.assertEqual(ocr.oldest_running_document_id, "doc_a")
            export = next(item for item in direct.processes if item.process_id == "export")
            self.assertEqual(export.waiting_count, 2)

            async with ProcessRuntimeClient(
                "http://runtime.test",
                transport=httpx.ASGITransport(app=app),
            ) as client:
                metrics = await client.get_queue_metrics(run_id="run_metrics")
                self.assertEqual(metrics.run_id, "run_metrics")
                self.assertEqual(metrics.process_group_count, 2)
                self.assertEqual(metrics.missing_worker_count, 1)
                self.assertEqual(metrics.missing_worker_process_count, 1)
                self.assertEqual(metrics.saturated_process_count, 1)
                self.assertEqual(metrics.processes[0].process_id, "ocr")

        asyncio.run(run_client())

    def test_runtime_queue_metrics_report_missing_worker_coverage(self) -> None:
        pipeline = PipelineSpec(
            id="worker_gap_flow",
            steps=[
                ProcessSpec(
                    id="extract",
                    capability="extract_text",
                    adapter=AdapterSpec(kind="queue", queue="gap.extract"),
                ),
            ],
        )
        store = InMemoryStateStore()
        service = RuntimeService(
            registry=PipelineRegistry([pipeline]),
            store=store,
        )

        async def run_check() -> None:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_worker_gap",
                    pipeline_id="worker_gap_flow",
                    documents=[
                        RuntimeDocumentInput(document_id="doc_a"),
                        RuntimeDocumentInput(document_id="doc_b"),
                    ],
                )
            )

            missing = await service.queue_metrics("run_worker_gap")
            self.assertEqual(missing.missing_worker_count, 2)
            self.assertEqual(missing.missing_worker_process_count, 1)
            process = missing.processes[0]
            self.assertEqual(process.process_id, "extract")
            self.assertEqual(process.queued_count, 2)
            self.assertEqual(process.missing_worker_count, 2)
            self.assertEqual(process.matching_worker_count, 0)
            self.assertEqual(process.healthy_worker_count, 0)
            self.assertTrue(process.missing_worker)

            await service.record_worker_heartbeat(
                run_id="run_worker_gap",
                worker_id="wrong-worker",
                pipeline_id="worker_gap_flow",
                adapter_kind="queue",
                capabilities=["classify_text"],
            )
            still_missing = await service.queue_metrics("run_worker_gap")
            self.assertEqual(still_missing.missing_worker_count, 2)
            self.assertEqual(still_missing.processes[0].matching_worker_count, 0)

            await service.record_worker_heartbeat(
                run_id="run_worker_gap",
                worker_id="extract-worker",
                pipeline_id="worker_gap_flow",
                adapter_kind="queue",
                capabilities=["extract_text"],
                status=RuntimeWorkerStatus.idle,
            )
            covered = await service.queue_metrics("run_worker_gap")
            self.assertEqual(covered.missing_worker_count, 0)
            self.assertEqual(covered.missing_worker_process_count, 0)
            process = covered.processes[0]
            self.assertEqual(process.matching_worker_count, 1)
            self.assertEqual(process.healthy_worker_count, 1)
            self.assertFalse(process.missing_worker)

        asyncio.run(run_check())

    def test_runtime_resources_gate_claims_and_missing_worker_coverage(self) -> None:
        pipeline = PipelineSpec(
            id="resource_flow",
            steps=[
                ProcessSpec(
                    id="ocr",
                    capability="ocr_pdf",
                    adapter=AdapterSpec(kind="queue", queue="resource.ocr"),
                    resources=ResourceSpec(
                        gpu_count=1,
                        memory_mb=2048,
                        labels=["cuda"],
                        units={"ocr_slots": 2},
                    ),
                ),
            ],
        )
        store = InMemoryStateStore()
        service = RuntimeService(
            registry=PipelineRegistry([pipeline]),
            store=store,
        )
        app = FastAPI()
        app.include_router(create_runtime_router(service), prefix="/api")

        async def run_check() -> None:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_resource_gate",
                    pipeline_id="resource_flow",
                    documents=[RuntimeDocumentInput(document_id="doc_a")],
                )
            )

            blocked = await service.claim_next(
                run_id="run_resource_gate",
                pipeline_id="resource_flow",
                worker_id="small-worker",
                adapter_kind="queue",
                capabilities=["ocr_pdf"],
            )
            self.assertIsNone(blocked)

            async with ProcessRuntimeClient(
                "http://runtime.test",
                transport=httpx.ASGITransport(app=app),
            ) as client:
                heartbeat = await client.worker_heartbeat(
                    run_id="run_resource_gate",
                    worker_id="gpu-no-label",
                    pipeline_id="resource_flow",
                    adapter_kind="queue",
                    capabilities=["ocr_pdf"],
                    resources={"gpu_count": 1, "memory_mb": 4096, "units": {"ocr_slots": 2}},
                )
                self.assertEqual(heartbeat["resources"]["gpu_count"], 1)

                missing = await client.get_queue_metrics(run_id="run_resource_gate")
                process = missing.processes[0]
                self.assertEqual(process.resources.gpu_count, 1)
                self.assertEqual(process.resources.labels, ["cuda"])
                self.assertEqual(process.missing_worker_count, 1)
                self.assertEqual(process.matching_worker_count, 0)

                await client.worker_heartbeat(
                    run_id="run_resource_gate",
                    worker_id="gpu-worker",
                    pipeline_id="resource_flow",
                    adapter_kind="queue",
                    capabilities=["ocr_pdf"],
                    resources=ResourceSpec(
                        gpu_count=1,
                        memory_mb=4096,
                        labels=["cuda"],
                        units={"ocr_slots": 2},
                    ),
                )
                covered = await client.get_queue_metrics(run_id="run_resource_gate")
                self.assertEqual(covered.missing_worker_count, 0)
                self.assertEqual(covered.processes[0].matching_worker_count, 1)

                claim = await client.claim_next(
                    run_id="run_resource_gate",
                    pipeline_id="resource_flow",
                    worker_id="gpu-worker",
                    adapter_kind="queue",
                    capabilities=["ocr_pdf"],
                    resources={
                        "gpu_count": 1,
                        "memory_mb": 4096,
                        "labels": ["cuda"],
                        "units": {"ocr_slots": 2},
                    },
                )
                self.assertIsNotNone(claim)
                assert claim is not None
                self.assertEqual(claim.process.id, "ocr")
                self.assertEqual(claim.process.resources.gpu_count, 1)

        asyncio.run(run_check())

    def test_runtime_resource_pool_quota_blocks_claims_and_reports_health(self) -> None:
        pipeline = PipelineSpec(
            id="quota_flow",
            steps=[
                ProcessSpec(
                    id="ocr",
                    capability="ocr_pdf",
                    adapter=AdapterSpec(kind="queue", queue="quota.ocr"),
                    resource_pool="gpu_pool",
                    resources=ResourceSpec(gpu_count=1, memory_mb=1024),
                ),
            ],
        )
        store = InMemoryStateStore()
        service = RuntimeService(
            registry=PipelineRegistry([pipeline]),
            store=store,
        )

        async def run_check() -> None:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_quota",
                    pipeline_id="quota_flow",
                    config={
                        "resource_pools": {
                            "gpu_pool": {
                                "gpu_count": 1,
                                "memory_mb": 1024,
                            }
                        }
                    },
                    documents=[
                        RuntimeDocumentInput(document_id="doc_a"),
                        RuntimeDocumentInput(document_id="doc_b"),
                    ],
                )
            )
            await service.record_worker_heartbeat(
                run_id="run_quota",
                worker_id="gpu-worker",
                pipeline_id="quota_flow",
                adapter_kind="queue",
                capabilities=["ocr_pdf"],
                resources={"gpu_count": 1, "memory_mb": 4096},
            )
            first = await service.claim_next(
                run_id="run_quota",
                pipeline_id="quota_flow",
                worker_id="gpu-worker",
                adapter_kind="queue",
                capabilities=["ocr_pdf"],
                resources={"gpu_count": 1, "memory_mb": 4096},
            )
            self.assertIsNotNone(first)
            assert first is not None
            self.assertEqual(first.document_id, "doc_a")
            self.assertEqual(first.process.resource_pool, "gpu_pool")

            blocked = await service.claim_next(
                run_id="run_quota",
                pipeline_id="quota_flow",
                worker_id="gpu-worker",
                adapter_kind="queue",
                capabilities=["ocr_pdf"],
                resources={"gpu_count": 1, "memory_mb": 4096},
            )
            self.assertIsNone(blocked)

            metrics = await service.queue_metrics("run_quota")
            self.assertEqual(metrics.missing_worker_count, 0)
            self.assertEqual(metrics.resource_blocked_count, 1)
            self.assertEqual(metrics.resource_blocked_process_count, 1)
            process = metrics.processes[0]
            self.assertEqual(process.resource_pool, "gpu_pool")
            self.assertEqual(process.resource_blocked_count, 1)
            self.assertTrue(process.resource_blocked)
            pool = metrics.resource_pools[0]
            self.assertEqual(pool.id, "gpu_pool")
            self.assertEqual(pool.limit.gpu_count, 1)
            self.assertEqual(pool.used.gpu_count, 1)
            self.assertEqual(pool.remaining.gpu_count, 0)
            self.assertEqual(pool.running_count, 1)
            self.assertEqual(pool.queued_count, 1)
            self.assertTrue(pool.saturated)

            health = await service.run_health("run_quota")
            codes = [issue.code for issue in health.issues]
            self.assertIn("resource_quota_blocked", codes)
            self.assertIn("resource_pool_saturated", codes)
            self.assertEqual(health.status, "warning")

            await service.put_process_output(
                run_id="run_quota",
                document_id="doc_a",
                process_id="ocr",
                output=ProcessOutput(values={"ok": True}),
                pipeline_id="quota_flow",
            )
            await store.set_status(
                run_id="run_quota",
                document_id="doc_a",
                process_id="ocr",
                status=ProcessStatus.completed,
            )
            await store.clear_claim(
                run_id="run_quota",
                document_id="doc_a",
                process_id="ocr",
            )
            released = await service.claim_next(
                run_id="run_quota",
                pipeline_id="quota_flow",
                worker_id="gpu-worker",
                adapter_kind="queue",
                capabilities=["ocr_pdf"],
                resources={"gpu_count": 1, "memory_mb": 4096},
            )
            self.assertIsNotNone(released)
            assert released is not None
            self.assertEqual(released.document_id, "doc_b")

        asyncio.run(run_check())

    def test_runtime_queue_metrics_report_worker_demand_for_scaling(self) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="scale_pkg",
                pipelines=["scale.yaml"],
                operation_types=[OperationTypeSpec(id="extract")],
                capabilities=[CapabilitySpec(id="ocr_pdf", operation_type="extract")],
                workers=[
                    WorkflowWorkerSpec(
                        id="ocr_worker",
                        capabilities=["ocr_pdf"],
                        pipeline_id="scale_flow",
                        process_id="ocr",
                        command=["worker"],
                    )
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="scale_flow",
                steps=[
                    ProcessSpec(
                        id="ocr",
                        capability="ocr_pdf",
                        max_concurrency=3,
                        adapter=AdapterSpec(kind="queue", queue="scale.ocr"),
                    ),
                ],
            ),
            package_id="scale_pkg",
        )
        registry.validate_package_workers("scale_pkg")
        store = InMemoryStateStore()
        service = RuntimeService(registry=registry, store=store)

        async def run_check() -> None:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_scale",
                    pipeline_id="scale_flow",
                    documents=[
                        RuntimeDocumentInput(document_id=f"doc_{index}")
                        for index in range(5)
                    ],
                )
            )
            await service.record_worker_heartbeat(
                run_id="run_scale",
                worker_id="ocr-worker-1",
                pipeline_id="scale_flow",
                adapter_kind="queue",
                capabilities=["ocr_pdf"],
            )

            metrics = await service.queue_metrics("run_scale")
            self.assertEqual(metrics.worker_demand_process_count, 1)
            self.assertEqual(metrics.worker_deficit_count, 2)
            demand = metrics.worker_demands[0]
            self.assertEqual(demand.pipeline_id, "scale_flow")
            self.assertEqual(demand.process_id, "ocr")
            self.assertEqual(demand.operation_type, "extract")
            self.assertEqual(demand.queued_count, 5)
            self.assertEqual(demand.claimable_queued_count, 5)
            self.assertEqual(demand.target_worker_count, 3)
            self.assertEqual(demand.healthy_worker_count, 1)
            self.assertEqual(demand.worker_deficit_count, 2)
            self.assertEqual(demand.package_worker_ids, ["ocr_worker"])

            capability_demands = await service.capability_demands("run_scale")
            self.assertEqual(capability_demands.count, 1)
            self.assertEqual(capability_demands.claimable_queued_count, 5)
            self.assertEqual(capability_demands.target_worker_count, 3)
            self.assertEqual(capability_demands.worker_deficit_count, 2)
            capability_demand = capability_demands.demands[0]
            self.assertEqual(capability_demand.capability, "ocr_pdf")
            self.assertEqual(capability_demand.operation_type, "extract")
            self.assertEqual(capability_demand.adapter_kind, "queue")
            self.assertEqual(capability_demand.pipeline_ids, ["scale_flow"])
            self.assertEqual(capability_demand.process_ids, ["ocr"])
            self.assertEqual(capability_demand.process_group_count, 1)
            self.assertEqual(capability_demand.queued_count, 5)
            self.assertEqual(capability_demand.target_worker_count, 3)
            self.assertEqual(capability_demand.healthy_worker_count, 1)
            self.assertEqual(capability_demand.worker_deficit_count, 2)
            self.assertEqual(capability_demand.package_worker_ids, ["ocr_worker"])

            app = FastAPI()
            app.include_router(create_runtime_router(service), prefix="/api")
            async with ProcessRuntimeClient(
                "http://testserver",
                transport=httpx.ASGITransport(app=app),
            ) as client:
                api_demands = await client.get_capability_demands(run_id="run_scale")
                self.assertEqual(api_demands.count, 1)
                self.assertEqual(api_demands.demands[0].capability, "ocr_pdf")
                self.assertEqual(api_demands.demands[0].operation_type, "extract")
                self.assertEqual(api_demands.demands[0].worker_deficit_count, 2)

                prometheus = await client.get_prometheus_metrics(run_id="run_scale")
                self.assertIn("fala_runtime_worker_target_workers", prometheus)
                self.assertIn('run_id="run_scale"', prometheus)
                self.assertIn('operation_type="extract"', prometheus)
                self.assertIn('package_worker_id="ocr_worker"', prometheus)
                self.assertIn("fala_runtime_capability_worker_deficit", prometheus)

        asyncio.run(run_check())

    def test_queue_bridge_exports_runs_and_applies_external_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            worker_script = root / "queue_worker.py"
            worker_script.write_text(
                textwrap.dedent(
                    """
                    import json
                    import sys

                    ctx = json.loads(sys.stdin.read())
                    print(json.dumps({
                        "values": {
                            "document_id": ctx["document_id"],
                            "process_id": ctx["process_id"],
                            "attempt": ctx["attempt"]
                        },
                        "metadata": {"worker": "jsonl-bridge"}
                    }))
                    """
                ).strip(),
                encoding="utf-8",
            )
            work_file = root / "work.jsonl"
            result_file = root / "results.jsonl"
            pipeline = PipelineSpec(
                id="bridge_flow",
                steps=[
                    ProcessSpec(
                        id="extract",
                        capability="extract_text",
                        adapter=AdapterSpec(kind="queue", queue="bridge.extract"),
                    )
                ],
            )
            service = RuntimeService(
                registry=PipelineRegistry([pipeline]),
                store=InMemoryStateStore(),
            )
            app = FastAPI()
            app.include_router(create_runtime_router(service), prefix="/api")

            class FileTransport:
                async def publish_work(self, envelope: QueueWorkEnvelope) -> None:
                    write_jsonl([envelope], work_file)

                async def publish_result(self, envelope: QueueResultEnvelope) -> None:
                    write_jsonl([envelope], result_file)

            async def exercise() -> None:
                await service.create_run_with_documents(
                    RuntimeRunInput(
                        run_id="run_bridge",
                        pipeline_id="bridge_flow",
                        documents=[RuntimeDocumentInput(document_id="doc_bridge")],
                    )
                )
                async with ProcessRuntimeClient(
                    "http://runtime.test",
                    transport=httpx.ASGITransport(app=app),
                ) as client:
                    exported = await export_claims_to_queue(
                        client,
                        FileTransport(),
                        run_id="run_bridge",
                        pipeline_id="bridge_flow",
                        worker_id="bridge-publisher",
                        capabilities=["extract_text"],
                        max_claims=1,
                    )
                    self.assertEqual(len(exported), 1)
                    self.assertEqual(exported[0].queue, "bridge.extract")
                    self.assertEqual(exported[0].context.document_id, "doc_bridge")

                    work = read_work_jsonl(work_file)
                    self.assertEqual(work[0].id, exported[0].id)
                    cli_result_file = root / "cli-results.jsonl"
                    buffer = StringIO()
                    cli_args = [
                        "queue-run-work",
                        "--work-file",
                        str(work_file),
                        "--result-file",
                        str(cli_result_file),
                        "--command",
                        sys.executable,
                        str(worker_script),
                    ]

                    def run_cli() -> int:
                        with redirect_stdout(buffer):
                            return runtime_cli_main(cli_args)

                    code = await asyncio.to_thread(run_cli)
                    self.assertEqual(code, 0, buffer.getvalue())
                    cli_results = read_result_jsonl(cli_result_file)
                    self.assertEqual(len(cli_results), 1)
                    self.assertEqual(cli_results[0].work_id, exported[0].id)
                    adapters = AdapterRegistry.default()
                    adapters.register(
                        "queue",
                        ExternalCommandAdapter(
                            command=[sys.executable, str(worker_script)],
                        ),
                    )
                    result = await run_queue_work(work[0], adapters=adapters)
                    self.assertEqual(result.status, ProcessStatus.completed)
                    self.assertIsNotNone(result.output)
                    assert result.output is not None
                    self.assertEqual(result.output.values["document_id"], "doc_bridge")
                    result.events.append(
                        ProcessEvent(
                            run_id="run_bridge",
                            document_id="doc_bridge",
                            process_id="extract",
                            type="process.progress",
                            status=ProcessStatus.running,
                            data={"percent": 100},
                        )
                    )
                    write_jsonl([result], result_file)

                    applied = await apply_queue_results(
                        client,
                        read_result_jsonl(result_file),
                    )
                    self.assertEqual(len(applied), 1)
                    duplicate = await apply_queue_results(
                        client,
                        read_result_jsonl(result_file),
                    )
                    self.assertTrue(duplicate[0]["duplicate"])
                    self.assertEqual(duplicate[0]["current_status"], "completed")
                    events = await service.store.list_events(
                        run_id="run_bridge",
                        document_id="doc_bridge",
                        process_id="extract",
                    )
                    progress = [
                        event for event in events if event.type == "process.progress"
                    ]
                    self.assertEqual(len(progress), 1)

                output = await service.store.get_output(
                    run_id="run_bridge",
                    document_id="doc_bridge",
                    process_id="extract",
                )
                self.assertIsNotNone(output)
                assert output is not None
                self.assertEqual(output.values["process_id"], "extract")
                statuses = await service.store.list_statuses(
                    run_id="run_bridge",
                    document_id="doc_bridge",
                )
                self.assertEqual(statuses["extract"], ProcessStatus.completed)

            asyncio.run(exercise())

    def test_sqlite_queue_transport_leases_runs_and_applies_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            worker_script = root / "sqlite_queue_worker.py"
            worker_script.write_text(
                textwrap.dedent(
                    """
                    import json
                    import sys

                    ctx = json.loads(sys.stdin.read())
                    print(json.dumps({
                        "values": {
                            "document_id": ctx["document_id"],
                            "process_id": ctx["process_id"],
                            "attempt": ctx["attempt"]
                        },
                        "metadata": {"worker": "sqlite-broker"}
                    }))
                    """
                ).strip(),
                encoding="utf-8",
            )
            queue_db = root / "broker.sqlite"
            pipeline = PipelineSpec(
                id="sqlite_bridge_flow",
                steps=[
                    ProcessSpec(
                        id="extract",
                        capability="extract_text",
                        adapter=AdapterSpec(kind="queue", queue="bridge.extract"),
                    )
                ],
            )
            service = RuntimeService(
                registry=PipelineRegistry([pipeline]),
                store=InMemoryStateStore(),
            )
            app = FastAPI()
            app.include_router(create_runtime_router(service), prefix="/api")

            async def exercise() -> None:
                await service.create_run_with_documents(
                    RuntimeRunInput(
                        run_id="run_sqlite_bridge",
                        pipeline_id="sqlite_bridge_flow",
                        documents=[RuntimeDocumentInput(document_id="doc_bridge")],
                    )
                )
                transport = SQLiteQueueTransport(queue_db)
                async with ProcessRuntimeClient(
                    "http://runtime.test",
                    transport=httpx.ASGITransport(app=app),
                ) as client:
                    exported = await export_claims_to_queue(
                        client,
                        transport,
                        run_id="run_sqlite_bridge",
                        pipeline_id="sqlite_bridge_flow",
                        worker_id="bridge-publisher",
                        capabilities=["extract_text"],
                        max_claims=1,
                    )
                    self.assertEqual(len(exported), 1)
                    self.assertEqual(exported[0].queue, "bridge.extract")

                    ready_work = await transport.load_work(
                        queue="bridge.extract",
                        state="ready",
                    )
                    self.assertEqual([item.id for item in ready_work], [exported[0].id])

                    buffer = StringIO()
                    cli_args = [
                        "queue-run-work",
                        "--queue-db",
                        str(queue_db),
                        "--queue",
                        "bridge.extract",
                        "--worker-id",
                        "sqlite-worker",
                        "--max-claims",
                        "1",
                        "--command",
                        sys.executable,
                        str(worker_script),
                    ]

                    def run_cli() -> int:
                        with redirect_stdout(buffer):
                            return runtime_cli_main(cli_args)

                    code = await asyncio.to_thread(run_cli)
                    self.assertEqual(code, 0, buffer.getvalue())
                    payload = json.loads(buffer.getvalue())
                    self.assertEqual(payload["processed_count"], 1)
                    self.assertEqual(payload["work_ids"], [exported[0].id])

                    completed_work = await transport.load_work(
                        queue="bridge.extract",
                        state="completed",
                    )
                    self.assertEqual([item.id for item in completed_work], [exported[0].id])
                    pending_results = await transport.load_results(
                        queue="bridge.extract",
                    )
                    self.assertEqual(len(pending_results), 1)
                    self.assertEqual(pending_results[0].work_id, exported[0].id)

                    applied = await apply_queue_results(client, pending_results)
                    self.assertEqual(len(applied), 1)
                    for result in pending_results:
                        await transport.mark_result_applied(result.id)
                    self.assertEqual(
                        await transport.load_results(queue="bridge.extract"),
                        [],
                    )

                output = await service.store.get_output(
                    run_id="run_sqlite_bridge",
                    document_id="doc_bridge",
                    process_id="extract",
                )
                self.assertIsNotNone(output)
                assert output is not None
                self.assertEqual(output.values["process_id"], "extract")
                self.assertEqual(output.metadata["worker"], "sqlite-broker")
                statuses = await service.store.list_statuses(
                    run_id="run_sqlite_bridge",
                    document_id="doc_bridge",
                )
                self.assertEqual(statuses["extract"], ProcessStatus.completed)

            asyncio.run(exercise())

    def test_sqlite_queue_transport_dead_letters_poison_work(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            queue_db = Path(tmp) / "broker.sqlite"
            transport = SQLiteQueueTransport(queue_db)
            claim = ClaimedProcess(
                pipeline_id="pipeline",
                run_id="run_poison",
                document_id="doc_poison",
                process=ScheduledProcess(
                    id="extract",
                    needs=[],
                    adapter={"kind": "queue", "queue": "poison.extract"},
                ),
                worker_id=None,
                attempt=1,
                claim_expires_at=datetime.now(timezone.utc) + timedelta(seconds=60),
                context=ProcessExecutionContext(
                    pipeline_id="pipeline",
                    run_id="run_poison",
                    document_id="doc_poison",
                    process_id="extract",
                    attempt=1,
                    input=ProcessInput(),
                ),
            )

            async def exercise() -> None:
                work = QueueWorkEnvelope.from_claim(claim)
                await transport.publish_work(work)

                first = await transport.claim_work(
                    queue="poison.extract",
                    worker_id="poison-worker",
                    lease_seconds=60,
                    max_deliveries=2,
                )
                self.assertIsNotNone(first)
                assert first is not None
                await transport.release_work(first.id, error="worker crashed")

                second = await transport.claim_work(
                    queue="poison.extract",
                    worker_id="poison-worker",
                    lease_seconds=60,
                    max_deliveries=2,
                )
                self.assertIsNotNone(second)
                assert second is not None
                await transport.release_work(second.id, error="worker crashed again")

                third = await transport.claim_work(
                    queue="poison.extract",
                    worker_id="poison-worker",
                    lease_seconds=60,
                    max_deliveries=2,
                )
                self.assertIsNone(third)
                dead = await transport.load_work(
                    queue="poison.extract",
                    state="dead_letter",
                )
                self.assertEqual([item.id for item in dead], [work.id])
                records = await transport.list_work_records(
                    queue="poison.extract",
                    state="dead_letter",
                    include_payload=True,
                )
                self.assertEqual(len(records), 1)
                record = records[0]
                self.assertEqual(record.id, work.id)
                self.assertEqual(record.delivery_count, 2)
                self.assertEqual(record.last_error, "max_deliveries exceeded (2/2)")
                self.assertIsNotNone(record.work)
                assert record.work is not None
                self.assertEqual(record.work.id, work.id)
                stats = await transport.stats()
                self.assertIn(
                    {
                        "queue": "poison.extract",
                        "state": "dead_letter",
                        "count": 1,
                    },
                    stats["work"],
                )

                list_buffer = StringIO()
                list_args = [
                    "queue-list-work",
                    "--queue-db",
                    str(queue_db),
                    "--queue",
                    "poison.extract",
                    "--state",
                    "dead_letter",
                    "--include-payload",
                ]

                def run_list_cli() -> int:
                    with redirect_stdout(list_buffer):
                        return runtime_cli_main(list_args)

                list_code = await asyncio.to_thread(run_list_cli)
                self.assertEqual(list_code, 0, list_buffer.getvalue())
                list_payload = json.loads(list_buffer.getvalue())
                self.assertEqual(list_payload["work_count"], 1)
                self.assertEqual(list_payload["work"][0]["id"], work.id)
                self.assertEqual(list_payload["work"][0]["state"], "dead_letter")
                self.assertEqual(list_payload["work"][0]["work"]["id"], work.id)

                requeue_buffer = StringIO()
                requeue_args = [
                    "queue-requeue-work",
                    "--queue-db",
                    str(queue_db),
                    "--work-id",
                    work.id,
                ]

                def run_requeue_cli() -> int:
                    with redirect_stdout(requeue_buffer):
                        return runtime_cli_main(requeue_args)

                requeue_code = await asyncio.to_thread(run_requeue_cli)
                self.assertEqual(requeue_code, 0, requeue_buffer.getvalue())
                requeue_payload = json.loads(requeue_buffer.getvalue())
                self.assertTrue(requeue_payload["ok"])
                self.assertEqual(requeue_payload["work_id"], work.id)
                ready_records = await transport.list_work_records(
                    queue="poison.extract",
                    state="ready",
                    include_payload=True,
                )
                self.assertEqual([item.id for item in ready_records], [work.id])
                self.assertEqual(ready_records[0].delivery_count, 0)
                self.assertIsNone(ready_records[0].last_error)

                requeued = await transport.claim_work(
                    queue="poison.extract",
                    worker_id="poison-worker",
                    lease_seconds=60,
                    max_deliveries=2,
                )
                self.assertIsNotNone(requeued)
                assert requeued is not None
                self.assertEqual(requeued.id, work.id)

            asyncio.run(exercise())

    def test_memory_queue_transport_shares_named_broker_and_tracks_results(self) -> None:
        target = f"memory://runtime-test-{uuid.uuid4().hex}"
        transport = create_queue_broker_transport(target)
        second_transport = create_queue_broker_transport(target)
        self.assertIsInstance(transport, MemoryQueueTransport)
        self.assertIsInstance(second_transport, MemoryQueueTransport)
        claim = ClaimedProcess(
            pipeline_id="pipeline",
            run_id="run_memory_broker",
            document_id="doc_memory_broker",
            process=ScheduledProcess(
                id="extract",
                needs=[],
                adapter={"kind": "queue", "queue": "memory.extract"},
            ),
            worker_id=None,
            attempt=1,
            claim_expires_at=datetime.now(timezone.utc) + timedelta(seconds=60),
            context=ProcessExecutionContext(
                pipeline_id="pipeline",
                run_id="run_memory_broker",
                document_id="doc_memory_broker",
                process_id="extract",
                attempt=1,
                input=ProcessInput(),
            ),
        )

        async def exercise() -> None:
            with patch.dict(os.environ, {"FALA_QUEUE_BROKER": target}):
                env_transport = create_queue_broker_transport()
            work = QueueWorkEnvelope.from_claim(claim)
            await transport.publish_work(work)

            first = await second_transport.claim_work(
                queue="memory.extract",
                worker_id="memory-worker",
                lease_seconds=60,
                max_deliveries=2,
            )
            self.assertIsNotNone(first)
            assert first is not None
            self.assertEqual(first.id, work.id)
            leased = await env_transport.list_work_records(
                queue="memory.extract",
                state="leased",
                include_payload=True,
            )
            self.assertEqual([record.id for record in leased], [work.id])
            self.assertEqual(leased[0].lease_owner, "memory-worker")
            await second_transport.release_work(first.id, error="boom")

            second = await second_transport.claim_work(
                queue="memory.extract",
                worker_id="memory-worker",
                lease_seconds=60,
                max_deliveries=2,
            )
            self.assertIsNotNone(second)
            assert second is not None
            await second_transport.release_work(second.id, error="boom again")

            self.assertIsNone(
                await second_transport.claim_work(
                    queue="memory.extract",
                    worker_id="memory-worker",
                    lease_seconds=60,
                    max_deliveries=2,
                )
            )
            dead = await transport.list_work_records(
                queue="memory.extract",
                state="dead_letter",
                include_payload=True,
            )
            self.assertEqual([record.id for record in dead], [work.id])
            self.assertEqual(dead[0].delivery_count, 2)
            self.assertEqual(dead[0].last_error, "max_deliveries exceeded (2/2)")

            requeued = await env_transport.requeue_work(
                work.id,
                reset_delivery_count=False,
            )
            self.assertIsNotNone(requeued)
            third = await second_transport.claim_work(
                queue="memory.extract",
                worker_id="memory-worker",
                lease_seconds=60,
                max_deliveries=3,
            )
            self.assertIsNotNone(third)
            assert third is not None
            result = QueueResultEnvelope.completed(
                third,
                ProcessOutput(values={"ok": True}),
            )
            await second_transport.publish_result(result)
            await second_transport.complete_work(third.id)

            results = await transport.load_results(queue="memory.extract")
            self.assertEqual([item.id for item in results], [result.id])
            await env_transport.mark_result_applied(result.id)
            self.assertEqual(await transport.load_results(queue="memory.extract"), [])
            stats = await transport.stats()
            self.assertIn(
                {"queue": "memory.extract", "state": "completed", "count": 1},
                stats["work"],
            )
            self.assertIn(
                {"queue": "memory.extract", "state": "applied", "count": 1},
                stats["results"],
            )

        asyncio.run(exercise())

    def test_redis_queue_transport_claims_requeues_and_tracks_results(self) -> None:
        client = _FakeRedisClient()
        transport = RedisQueueTransport(
            "redis://localhost/0?prefix=test&socket_timeout=1",
            client=client,
        )
        self.assertEqual(transport.prefix, "test")
        self.assertEqual(transport.target, "redis://localhost/0?socket_timeout=1")
        claim = ClaimedProcess(
            pipeline_id="pipeline",
            run_id="run_redis_broker",
            document_id="doc_redis_broker",
            process=ScheduledProcess(
                id="extract",
                needs=[],
                adapter={"kind": "queue", "queue": "redis.extract"},
            ),
            worker_id=None,
            attempt=1,
            claim_expires_at=datetime.now(timezone.utc) + timedelta(seconds=60),
            context=ProcessExecutionContext(
                pipeline_id="pipeline",
                run_id="run_redis_broker",
                document_id="doc_redis_broker",
                process_id="extract",
                attempt=1,
                input=ProcessInput(),
            ),
        )

        async def exercise() -> None:
            work = QueueWorkEnvelope.from_claim(claim)
            await transport.publish_work(work)

            first = await transport.claim_work(
                queue=None,
                worker_id="redis-worker",
                lease_seconds=60,
                max_deliveries=2,
            )
            self.assertIsNotNone(first)
            assert first is not None
            self.assertEqual(first.id, work.id)
            await transport.release_work(first.id, error="boom")

            second = await transport.claim_work(
                queue="redis.extract",
                worker_id="redis-worker",
                lease_seconds=60,
                max_deliveries=2,
            )
            self.assertIsNotNone(second)
            assert second is not None
            await transport.release_work(second.id, error="boom again")

            third = await transport.claim_work(
                queue="redis.extract",
                worker_id="redis-worker",
                lease_seconds=60,
                max_deliveries=2,
            )
            self.assertIsNone(third)
            records = await transport.list_work_records(
                queue="redis.extract",
                state="dead_letter",
                include_payload=True,
            )
            self.assertEqual([record.id for record in records], [work.id])
            self.assertEqual(records[0].delivery_count, 2)
            self.assertEqual(records[0].last_error, "max_deliveries exceeded (2/2)")
            self.assertIsNotNone(records[0].work)

            requeued = await transport.requeue_work(
                work.id,
                reset_delivery_count=False,
            )
            self.assertIsNotNone(requeued)
            final = await transport.claim_work(
                queue="redis.extract",
                worker_id="redis-worker",
                lease_seconds=60,
                max_deliveries=3,
            )
            self.assertIsNotNone(final)
            assert final is not None
            result = QueueResultEnvelope.completed(
                final,
                ProcessOutput(values={"ok": True}),
            )
            await transport.publish_result(result)
            await transport.complete_work(final.id)

            results = await transport.load_results(queue="redis.extract")
            self.assertEqual([item.id for item in results], [result.id])
            await transport.mark_result_applied(result.id)
            self.assertEqual(await transport.load_results(queue="redis.extract"), [])
            stats = await transport.stats()
            self.assertIn(
                {"queue": "redis.extract", "state": "completed", "count": 1},
                stats["work"],
            )
            self.assertIn(
                {"queue": "redis.extract", "state": "applied", "count": 1},
                stats["results"],
            )

        asyncio.run(exercise())
        self.assertTrue(any(key.startswith("test:") for key in client.hashes))
        self.assertNotIn("fala:queue_work", client.hashes)

    def test_create_queue_broker_transport_builds_redis_transport(self) -> None:
        client = _FakeRedisClient()
        with patch("fala.queue_bridge._default_redis_client", return_value=client):
            transport = create_queue_broker_transport(
                "redis://localhost/1?prefix=factory"
            )
        self.assertIsInstance(transport, RedisQueueTransport)
        assert isinstance(transport, RedisQueueTransport)
        self.assertEqual(transport.prefix, "factory")
        self.assertEqual(transport.target, "redis://localhost/1")

    def test_runtime_web_panel_inspects_and_requeues_broker_dead_letter_work(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            queue_db = Path(tmp) / "broker.sqlite"
            transport = SQLiteQueueTransport(queue_db)
            service = RuntimeService(
                registry=PipelineRegistry(
                    [
                        PipelineSpec(
                            id="web_broker_flow",
                            steps=[
                                ProcessSpec(
                                    id="extract",
                                    adapter=AdapterSpec(
                                        kind="queue",
                                        queue="broker.extract",
                                    ),
                                )
                            ],
                        )
                    ]
                ),
                store=InMemoryStateStore(),
            )
            app = create_runtime_web_app(service=service, queue_db=queue_db)
            claim = ClaimedProcess(
                pipeline_id="web_broker_flow",
                run_id="run_web_broker",
                document_id="doc_broker",
                process=ScheduledProcess(
                    id="extract",
                    needs=[],
                    adapter={"kind": "queue", "queue": "broker.extract"},
                ),
                worker_id=None,
                attempt=1,
                claim_expires_at=datetime.now(timezone.utc) + timedelta(seconds=60),
                context=ProcessExecutionContext(
                    pipeline_id="web_broker_flow",
                    run_id="run_web_broker",
                    document_id="doc_broker",
                    process_id="extract",
                    attempt=1,
                    input=ProcessInput(),
                ),
            )

            async def exercise() -> None:
                work = QueueWorkEnvelope.from_claim(claim)
                await transport.publish_work(work)
                first = await transport.claim_work(
                    queue="broker.extract",
                    worker_id="worker",
                    max_deliveries=1,
                )
                self.assertIsNotNone(first)
                assert first is not None
                await transport.release_work(first.id, error="boom")
                self.assertIsNone(
                    await transport.claim_work(
                        queue="broker.extract",
                        worker_id="worker",
                        max_deliveries=1,
                    )
                )

                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://testserver",
                ) as client:
                    queue_response = await client.get("/queue")
                    self.assertEqual(queue_response.status_code, 200)
                    self.assertIn("Broker queue", queue_response.text)
                    self.assertIn(work.id, queue_response.text)
                    self.assertIn("dead_letter", queue_response.text)
                    self.assertIn("doc_broker", queue_response.text)

                    partial_response = await client.get(
                        "/queue/broker",
                        params={"state": "dead_letter", "queue": "broker.extract"},
                    )
                    self.assertEqual(partial_response.status_code, 200)
                    self.assertIn("max_deliveries exceeded", partial_response.text)

                    requeue_response = await client.post(
                        f"/queue/broker/{work.id}/requeue",
                        params={"state": "dead_letter", "queue": "broker.extract"},
                    )
                    self.assertEqual(requeue_response.status_code, 200)
                    self.assertIn(f"Requeued {work.id}", requeue_response.text)
                    self.assertNotIn("max_deliveries exceeded", requeue_response.text)

                ready = await transport.list_work_records(
                    queue="broker.extract",
                    state="ready",
                    include_payload=True,
                )
                self.assertEqual([item.id for item in ready], [work.id])
                self.assertEqual(ready[0].delivery_count, 0)
                audit = await service.operator_audit(run_id="run_web_broker")
                self.assertEqual(audit.events[0].action, "queue.work.requeue")
                self.assertEqual(audit.events[0].target, f"queue-work:{work.id}")

            asyncio.run(exercise())

    def test_queue_work_renews_control_plane_claim_while_command_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            worker_script = Path(tmp) / "slow_queue_worker.py"
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
                run_id="run_queue_worker",
                document_id="doc_queue_worker",
                process=ScheduledProcess(
                    id="slow",
                    needs=[],
                    adapter={"kind": "queue", "queue": "slow.queue"},
                    timeout_seconds=30,
                ),
                worker_id="broker-worker",
                attempt=1,
                claim_expires_at=datetime.now(timezone.utc) + timedelta(seconds=60),
                context=ProcessExecutionContext(
                    pipeline_id="pipeline",
                    run_id="run_queue_worker",
                    document_id="doc_queue_worker",
                    process_id="slow",
                    attempt=1,
                    input=ProcessInput(),
                ),
            )
            work = QueueWorkEnvelope.from_claim(claim)
            client = _FakeRuntimeClient(None)
            adapters = AdapterRegistry.default()
            adapters.register(
                "queue",
                ExternalCommandAdapter(
                    command=[sys.executable, str(worker_script)],
                ),
            )

            result = asyncio.run(
                run_queue_work(
                    work,
                    adapters=adapters,
                    renew_client=client,  # type: ignore[arg-type]
                    renew_interval_seconds=0.01,
                    lease_seconds=60,
                )
            )

            self.assertEqual(result.status, ProcessStatus.completed)
            self.assertIsNotNone(result.output)
            assert result.output is not None
            self.assertEqual(result.output.values["ok"], True)
            self.assertGreaterEqual(len(client.renews), 1)
            self.assertEqual(client.renews[0]["run_id"], "run_queue_worker")
            self.assertEqual(client.renews[0]["document_id"], "doc_queue_worker")
            self.assertEqual(client.renews[0]["process_id"], "slow")
            self.assertEqual(client.renews[0]["pipeline_id"], "pipeline")
            self.assertEqual(client.renews[0]["worker_id"], "broker-worker")

    def test_queue_unassigned_claim_can_be_taken_by_broker_worker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            worker_script = Path(tmp) / "broker_worker.py"
            worker_script.write_text(
                textwrap.dedent(
                    """
                    import json
                    import sys

                    ctx = json.loads(sys.stdin.read())
                    print(json.dumps({
                        "values": {"document_id": ctx["document_id"]},
                        "metadata": {"worker": "real-broker-worker"}
                    }))
                    """
                ),
                encoding="utf-8",
            )
            pipeline = PipelineSpec(
                id="unassigned_broker_flow",
                steps=[
                    ProcessSpec(
                        id="extract",
                        capability="extract_text",
                        adapter=AdapterSpec(kind="queue", queue="broker.extract"),
                    )
                ],
            )
            service = RuntimeService(
                registry=PipelineRegistry([pipeline]),
                store=InMemoryStateStore(),
            )
            app = FastAPI()
            app.include_router(create_runtime_router(service), prefix="/api")

            class CollectingTransport:
                def __init__(self) -> None:
                    self.work: list[QueueWorkEnvelope] = []

                async def publish_work(self, envelope: QueueWorkEnvelope) -> None:
                    self.work.append(envelope)

                async def publish_result(self, envelope: QueueResultEnvelope) -> None:
                    raise AssertionError("not used")

            async def exercise() -> None:
                await service.create_run_with_documents(
                    RuntimeRunInput(
                        run_id="run_unassigned_broker",
                        pipeline_id="unassigned_broker_flow",
                        documents=[RuntimeDocumentInput(document_id="doc_broker")],
                    )
                )
                transport = CollectingTransport()
                async with ProcessRuntimeClient(
                    "http://runtime.test",
                    transport=httpx.ASGITransport(app=app),
                ) as client:
                    exported = await export_claims_to_queue(
                        client,
                        transport,
                        run_id="run_unassigned_broker",
                        pipeline_id="unassigned_broker_flow",
                        worker_id=None,
                        capabilities=["extract_text"],
                        max_claims=1,
                        metadata={"publisher_worker_id": "publisher"},
                    )
                    self.assertEqual(len(exported), 1)
                    self.assertIsNone(exported[0].worker_id)

                    work = assign_queue_work_worker(exported[0], "real-broker-worker")
                    self.assertEqual(work.worker_id, "real-broker-worker")
                    self.assertTrue(work.metadata["worker_id_assigned_at_run"])

                    adapters = AdapterRegistry.default()
                    adapters.register(
                        "queue",
                        ExternalCommandAdapter(
                            command=[sys.executable, str(worker_script)],
                        ),
                    )
                    result = await run_queue_work(
                        work,
                        adapters=adapters,
                        renew_client=client,
                        lease_seconds=60,
                    )
                    applied = await apply_queue_results(client, [result])
                    self.assertTrue(applied[0]["ok"])

                output = await service.store.get_output(
                    run_id="run_unassigned_broker",
                    document_id="doc_broker",
                    process_id="extract",
                )
                self.assertIsNotNone(output)
                assert output is not None
                self.assertEqual(output.values["document_id"], "doc_broker")
                self.assertEqual(output.metadata["worker"], "real-broker-worker")
                events = await service.store.list_events(
                    run_id="run_unassigned_broker",
                    document_id="doc_broker",
                    process_id="extract",
                )
                renewed = [event for event in events if event.type == "process.claim_renewed"]
                self.assertEqual(renewed[0].data["worker_id"], "real-broker-worker")

            asyncio.run(exercise())

    def test_queue_failed_result_uses_control_plane_retry_policy(self) -> None:
        pipeline = PipelineSpec(
            id="queue_retry_flow",
            steps=[
                ProcessSpec(
                    id="extract",
                    capability="extract_text",
                    adapter=AdapterSpec(kind="queue", queue="retry.extract"),
                    retry=RetryPolicy(
                        max_attempts=3,
                        retry_error_kinds=["transient_io"],
                        terminal_error_kinds=["validation_error"],
                    ),
                )
            ],
        )
        service = RuntimeService(
            registry=PipelineRegistry([pipeline]),
            store=InMemoryStateStore(),
        )
        app = FastAPI()
        app.include_router(create_runtime_router(service), prefix="/api")

        class CollectingTransport:
            def __init__(self) -> None:
                self.work: list[QueueWorkEnvelope] = []

            async def publish_work(self, envelope: QueueWorkEnvelope) -> None:
                self.work.append(envelope)

            async def publish_result(self, envelope: QueueResultEnvelope) -> None:
                raise AssertionError("not used")

        async def exercise() -> None:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_queue_retry",
                    pipeline_id="queue_retry_flow",
                    documents=[RuntimeDocumentInput(document_id="doc_retry")],
                )
            )
            async with ProcessRuntimeClient(
                "http://runtime.test",
                transport=httpx.ASGITransport(app=app),
            ) as client:
                first_transport = CollectingTransport()
                exported = await export_claims_to_queue(
                    client,
                    first_transport,
                    run_id="run_queue_retry",
                    pipeline_id="queue_retry_flow",
                    worker_id="retry-worker",
                    capabilities=["extract_text"],
                    max_claims=1,
                )
                self.assertEqual(len(exported), 1)
                transient = QueueResultEnvelope.failed(
                    exported[0],
                    error="temporary outage",
                    error_kind="transient_io",
                )
                applied = await apply_queue_results(client, [transient])
                self.assertEqual(applied[0]["action"]["action"], "retry")
                transient_duplicate = await apply_queue_results(client, [transient])
                self.assertTrue(transient_duplicate[0]["duplicate"])
                self.assertEqual(transient_duplicate[0]["work_id"], transient.work_id)

                statuses = await service.store.list_statuses(
                    run_id="run_queue_retry",
                    document_id="doc_retry",
                )
                self.assertEqual(statuses["extract"], ProcessStatus.queued)
                events = await service.store.list_events(
                    run_id="run_queue_retry",
                    document_id="doc_retry",
                    process_id="extract",
                )
                self.assertIn("process.retry_scheduled", [event.type for event in events])

                second_transport = CollectingTransport()
                second = await export_claims_to_queue(
                    client,
                    second_transport,
                    run_id="run_queue_retry",
                    pipeline_id="queue_retry_flow",
                    worker_id="retry-worker",
                    capabilities=["extract_text"],
                    max_claims=1,
                )
                self.assertEqual(len(second), 1)
                terminal = QueueResultEnvelope.failed(
                    second[0],
                    error="bad document",
                    error_kind="validation_error",
                )
                terminal_applied = await apply_queue_results(client, [terminal])
                self.assertEqual(terminal_applied[0]["action"]["action"], "fail")
                terminal_duplicate = await apply_queue_results(client, [terminal])
                self.assertTrue(terminal_duplicate[0]["duplicate"])
                self.assertEqual(terminal_duplicate[0]["current_status"], "failed")

            statuses = await service.store.list_statuses(
                run_id="run_queue_retry",
                document_id="doc_retry",
            )
            self.assertEqual(statuses["extract"], ProcessStatus.failed)
            events = await service.store.list_events(
                run_id="run_queue_retry",
                document_id="doc_retry",
                process_id="extract",
            )
            failed = [event for event in events if event.type == "process.failed"][-1]
            self.assertEqual(failed.data["error_kind"], "validation_error")
            self.assertEqual(failed.data["terminal_reason"], "terminal_error_kind")

        asyncio.run(exercise())

    def test_runtime_api_and_client_expose_run_health(self) -> None:
        pipeline = PipelineSpec(
            id="health_flow",
            steps=[
                ProcessSpec(
                    id="extract",
                    capability="extract_text",
                    adapter=AdapterSpec(kind="queue", queue="health.extract"),
                ),
            ],
        )
        store = InMemoryStateStore()
        service = RuntimeService(
            registry=PipelineRegistry([pipeline]),
            store=store,
        )
        app = FastAPI()
        app.include_router(create_runtime_router(service), prefix="/api")

        async def run_client() -> None:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_health",
                    pipeline_id="health_flow",
                    documents=[RuntimeDocumentInput(document_id="doc_health")],
                )
            )

            async with ProcessRuntimeClient(
                "http://runtime.test",
                transport=httpx.ASGITransport(app=app),
            ) as client:
                health = await client.get_run_health(run_id="run_health")
                self.assertEqual(health.status, "critical")
                self.assertEqual(health.critical_count, 1)
                self.assertEqual(health.issues[0].code, "missing_worker")
                self.assertEqual(health.issues[0].process_id, "extract")
                self.assertEqual(health.metrics.missing_worker_count, 1)

                await client.worker_heartbeat(
                    run_id="run_health",
                    worker_id="extract-worker",
                    pipeline_id="health_flow",
                    adapter_kind="queue",
                    capabilities=["extract_text"],
                )
                covered = await client.get_run_health(run_id="run_health")
                self.assertEqual(covered.status, "healthy")
                self.assertEqual(covered.issue_count, 0)
                self.assertEqual(covered.healthy_worker_count, 1)

        asyncio.run(run_client())

    def test_runtime_stream_chunks_and_checkpoints_are_persisted(self) -> None:
        pipeline = PipelineSpec(
            id="stream_flow",
            steps=[
                ProcessSpec(
                    id="extract",
                    adapter=AdapterSpec(kind="queue", queue="stream.extract"),
                ),
            ],
        )

        async def run_check(store: InMemoryStateStore | SQLiteStateStore) -> None:
            service = RuntimeService(
                registry=PipelineRegistry([pipeline]),
                store=store,
            )
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_stream",
                    pipeline_id="stream_flow",
                    documents=[RuntimeDocumentInput(document_id="doc_stream")],
                )
            )
            first = await service.append_stream_chunk(
                run_id="run_stream",
                document_id="doc_stream",
                process_id="extract",
                stream_id="pages",
                kind="page",
                values={"page": "1"},
            )
            second = await service.append_stream_chunk(
                run_id="run_stream",
                document_id="doc_stream",
                process_id="extract",
                stream_id="pages",
                values={"page": "2"},
            )
            self.assertEqual(first.sequence, 0)
            self.assertEqual(second.sequence, 1)

            chunks = await service.list_stream_chunks(
                run_id="run_stream",
                document_id="doc_stream",
                process_id="extract",
                stream_id="pages",
                after_sequence=0,
            )
            self.assertEqual([chunk.sequence for chunk in chunks], [1])
            self.assertEqual(chunks[0].values["page"], "2")

            checkpoint = await service.put_stream_checkpoint(
                run_id="run_stream",
                document_id="doc_stream",
                process_id="extract",
                stream_id="pages",
                consumer_id="enrich",
                sequence=second.sequence,
                chunk_id=second.chunk_id,
            )
            self.assertEqual(checkpoint.sequence, 1)
            stored_checkpoint = await service.get_stream_checkpoint(
                run_id="run_stream",
                document_id="doc_stream",
                process_id="extract",
                stream_id="pages",
                consumer_id="enrich",
            )
            self.assertIsNotNone(stored_checkpoint)
            assert stored_checkpoint is not None
            self.assertEqual(stored_checkpoint.chunk_id, second.chunk_id)

            state = await service.load_state_model("run_stream")
            self.assertEqual(state.summary.stream_count, 1)
            self.assertEqual(state.summary.stream_chunk_count, 2)
            self.assertEqual(state.summary.stream_checkpoint_count, 1)
            step = state.documents[0].steps[0]
            self.assertEqual(step.stream_count, 1)
            self.assertEqual(step.stream_chunk_count, 2)
            self.assertEqual(step.stream_checkpoint_count, 1)
            self.assertEqual(step.streams[0].stream_id, "pages")
            self.assertEqual(step.streams[0].last_sequence, 1)
            self.assertEqual(step.streams[0].value_keys, ["page"])
            self.assertEqual(step.streams[0].checkpoint_consumers, ["enrich"])

            events = await store.list_events(
                run_id="run_stream",
                document_id="doc_stream",
                process_id="extract",
            )
            self.assertIn("process.stream.chunk", [event.type for event in events])
            self.assertIn("process.stream.checkpoint", [event.type for event in events])

        asyncio.run(run_check(InMemoryStateStore()))
        with tempfile.TemporaryDirectory() as tmp:
            asyncio.run(run_check(SQLiteStateStore(Path(tmp) / "runtime.db")))

    def test_runtime_stream_backpressure_limits_buffered_chunks(self) -> None:
        pipeline = PipelineSpec(
            id="stream_pressure_flow",
            steps=[
                ProcessSpec(
                    id="extract",
                    capability="extract_pages",
                    adapter=AdapterSpec(kind="queue", queue="stream.extract"),
                ),
            ],
        )
        package = WorkflowPackageSpec(
            id="stream_pressure_package",
            document_types=[DocumentTypeSpec(id="source_document")],
            operation_types=[OperationTypeSpec(id="extract")],
            capabilities=[
                CapabilitySpec(
                    id="extract_pages",
                    operation_type="extract",
                    accepts_document_types=["source_document"],
                    emits_streams=[
                        StreamSpec(
                            stream_id="pages",
                            kinds=["page"],
                            consumers=["chunker", "reviewer"],
                            max_buffered_chunks=2,
                            value_schema={
                                "type": "object",
                                "properties": {"text": {"type": "string"}},
                                "required": ["text"],
                                "additionalProperties": False,
                            },
                        )
                    ],
                )
            ],
            pipelines=["stream_pressure_flow.yaml"],
        )
        registry = PipelineRegistry()
        registry.add_package(package)
        registry.add(pipeline, package_id=package.id)
        store = InMemoryStateStore()
        service = RuntimeService(registry=registry, store=store)

        async def run_check() -> None:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_stream_pressure",
                    pipeline_id="stream_pressure_flow",
                    documents=[
                        RuntimeDocumentInput(
                            document_id="doc_pressure",
                            document_type="source_document",
                        )
                    ],
                )
            )
            first = await service.append_stream_chunk(
                run_id="run_stream_pressure",
                document_id="doc_pressure",
                process_id="extract",
                stream_id="pages",
                kind="page",
                values={"text": "one"},
            )
            second = await service.append_stream_chunk(
                run_id="run_stream_pressure",
                document_id="doc_pressure",
                process_id="extract",
                stream_id="pages",
                kind="page",
                values={"text": "two"},
            )
            self.assertEqual([first.sequence, second.sequence], [0, 1])

            with self.assertRaisesRegex(ValueError, "backpressure limit exceeded"):
                await service.append_stream_chunk(
                    run_id="run_stream_pressure",
                    document_id="doc_pressure",
                    process_id="extract",
                    stream_id="pages",
                    kind="page",
                    values={"text": "three"},
                )

            await service.put_stream_checkpoint(
                run_id="run_stream_pressure",
                document_id="doc_pressure",
                process_id="extract",
                stream_id="pages",
                consumer_id="chunker",
                sequence=0,
                chunk_id=first.chunk_id,
            )
            third = await service.append_stream_chunk(
                run_id="run_stream_pressure",
                document_id="doc_pressure",
                process_id="extract",
                stream_id="pages",
                kind="page",
                values={"text": "three"},
            )
            self.assertEqual(third.sequence, 2)

            state = await service.load_state_model("run_stream_pressure")
            stream = state.documents[0].steps[0].streams[0]
            self.assertEqual(stream.declared_consumers, ["chunker", "reviewer"])
            self.assertEqual(stream.checkpoint_lag, {"chunker": 2})
            self.assertEqual(stream.max_checkpoint_lag, 2)
            self.assertEqual(stream.min_checkpoint_sequence, 0)
            self.assertEqual(stream.max_checkpoint_sequence, 0)

            await store.put_stream_chunk(
                RuntimeStreamChunk(
                    run_id="run_stream_pressure",
                    document_id="doc_pressure",
                    process_id="extract",
                    stream_id="pages",
                    sequence=3,
                    kind="page",
                    values={"text": "forced"},
                )
            )
            health = await service.run_health("run_stream_pressure")
            issue = next(
                item for item in health.issues if item.code == "stream_backpressure"
            )
            self.assertEqual(issue.severity, "warning")
            self.assertEqual(issue.document_id, "doc_pressure")
            self.assertEqual(issue.process_id, "extract")
            self.assertEqual(issue.data["stream_id"], "pages")
            self.assertEqual(issue.data["max_buffered_chunks"], 2)
            self.assertEqual(issue.data["max_checkpoint_lag"], 3)

            lag_page = await service.stream_lag(
                "run_stream_pressure",
                consumer_id="chunker",
            )
            self.assertEqual(lag_page.count, 1)
            self.assertEqual(lag_page.max_lag, 3)
            self.assertEqual(lag_page.over_limit_count, 1)
            lag_item = lag_page.items[0]
            self.assertEqual(lag_item.document_id, "doc_pressure")
            self.assertEqual(lag_item.process_id, "extract")
            self.assertEqual(lag_item.operation_type, "extract")
            self.assertEqual(lag_item.stream_id, "pages")
            self.assertEqual(lag_item.consumer_id, "chunker")
            self.assertTrue(lag_item.declared_consumer)
            self.assertEqual(lag_item.lag, 3)
            self.assertEqual(lag_item.checkpoint_sequence, 0)
            self.assertEqual(lag_item.last_sequence, 3)
            self.assertEqual(lag_item.max_buffered_chunks, 2)
            self.assertTrue(lag_item.over_limit)

            uncheckpointed = await service.stream_lag(
                "run_stream_pressure",
                consumer_id="reviewer",
            )
            self.assertEqual(uncheckpointed.count, 1)
            self.assertTrue(uncheckpointed.items[0].uncheckpointed)
            self.assertTrue(uncheckpointed.items[0].declared_consumer)
            self.assertEqual(uncheckpointed.items[0].lag, 4)

            all_lag = await service.stream_lag("run_stream_pressure")
            lag_by_consumer = {item.consumer_id: item for item in all_lag.items}
            self.assertEqual(set(lag_by_consumer), {"chunker", "reviewer"})
            self.assertFalse(lag_by_consumer["chunker"].uncheckpointed)
            self.assertTrue(lag_by_consumer["reviewer"].uncheckpointed)
            self.assertEqual(lag_by_consumer["reviewer"].lag, 4)
            filtered_lag = await service.stream_lag(
                "run_stream_pressure",
                operation_type="extract",
            )
            self.assertEqual(filtered_lag.count, 2)
            self.assertEqual(filtered_lag.filters["operation_type"], "extract")
            empty_lag = await service.stream_lag(
                "run_stream_pressure",
                operation_type="render",
            )
            self.assertEqual(empty_lag.count, 0)

        asyncio.run(run_check())

    def test_runtime_api_and_client_expose_stream_chunks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "page.txt"
            source.write_text("page one", encoding="utf-8")
            pipeline = PipelineSpec(
                id="stream_api_flow",
                steps=[
                    ProcessSpec(
                        id="extract",
                        adapter=AdapterSpec(kind="queue", queue="stream.extract"),
                    ),
                ],
            )
            store = InMemoryStateStore()
            service = RuntimeService(
                registry=PipelineRegistry([pipeline]),
                store=store,
                artifact_roots=[root],
                artifact_store_root=root / "artifact-store",
            )
            app = FastAPI()
            app.include_router(create_runtime_router(service), prefix="/api")

            async def run_client() -> None:
                await service.create_run_with_documents(
                    RuntimeRunInput(
                        run_id="run_stream_api",
                        pipeline_id="stream_api_flow",
                        documents=[RuntimeDocumentInput(document_id="doc_stream")],
                    )
                )

                transport = httpx.ASGITransport(app=app)
                async with httpx.AsyncClient(
                    transport=transport,
                    base_url="http://runtime.test",
                ) as http_client:
                    client = ProcessRuntimeClient(
                        "http://runtime.test",
                        client=http_client,
                    )
                    first = await client.append_stream_chunk(
                        run_id="run_stream_api",
                        document_id="doc_stream",
                        process_id="extract",
                        stream_id="pages",
                        kind="page",
                        values={"text": "one"},
                        artifacts=[ArtifactRef(kind="page_text", uri=source.as_uri())],
                    )
                    self.assertEqual(first.artifacts[0].metadata["size_bytes"], 8)
                    self.assertTrue(first.artifacts[0].uri.startswith("fala-artifact://"))
                    download = await http_client.get(
                        "/api/runs/run_stream_api/process-runtime/doc_stream"
                        f"/processes/extract/streams/pages/chunks/{first.chunk_id}"
                        f"/artifacts/{first.artifacts[0].id}/download"
                    )
                    self.assertEqual(download.status_code, 200)
                    self.assertEqual(download.content, b"page one")

                    await client.append_stream_chunk(
                        run_id="run_stream_api",
                        document_id="doc_stream",
                        process_id="extract",
                        stream_id="pages",
                        values={"text": "two"},
                    )
                    chunks = await client.list_stream_chunks(
                        run_id="run_stream_api",
                        document_id="doc_stream",
                        process_id="extract",
                        stream_id="pages",
                        after_sequence=first.sequence,
                    )
                    self.assertEqual(len(chunks), 1)
                    self.assertEqual(chunks[0].values["text"], "two")

                    first_batch = await client.read_stream_batch(
                        run_id="run_stream_api",
                        document_id="doc_stream",
                        process_id="extract",
                        stream_id="pages",
                        consumer_id="writer",
                        limit=1,
                    )
                    self.assertIsNone(first_batch.checkpoint)
                    self.assertEqual(first_batch.after_sequence, -1)
                    self.assertEqual(first_batch.chunk_count, 1)
                    self.assertEqual(first_batch.chunks[0].values["text"], "one")
                    self.assertEqual(first_batch.last_sequence, first.sequence)
                    first_checkpoint = await client.commit_stream_batch(first_batch)
                    self.assertIsNotNone(first_checkpoint)
                    assert first_checkpoint is not None
                    self.assertEqual(first_checkpoint.sequence, 0)

                    lag_page = await client.stream_lag_page(
                        run_id="run_stream_api",
                        document_id="doc_stream",
                        process_id="extract",
                        stream_id="pages",
                        consumer_id="writer",
                    )
                    self.assertEqual(lag_page.count, 1)
                    self.assertEqual(lag_page.items[0].lag, 1)
                    self.assertEqual(lag_page.items[0].checkpoint_sequence, 0)
                    self.assertEqual(lag_page.items[0].last_sequence, 1)

                    lag_by_adapter = await client.stream_lag_page(
                        run_id="run_stream_api",
                        adapter_kind="queue",
                    )
                    self.assertEqual(lag_by_adapter.count, 1)
                    self.assertEqual(lag_by_adapter.items[0].consumer_id, "writer")

                    next_batch = await client.read_stream_batch(
                        run_id="run_stream_api",
                        document_id="doc_stream",
                        process_id="extract",
                        stream_id="pages",
                        consumer_id="writer",
                        limit=10,
                    )
                    self.assertIsNotNone(next_batch.checkpoint)
                    assert next_batch.checkpoint is not None
                    self.assertEqual(next_batch.checkpoint.sequence, 0)
                    self.assertEqual(next_batch.chunk_count, 1)
                    self.assertEqual(next_batch.chunks[0].values["text"], "two")
                    checkpoint = await client.commit_stream_batch(
                        next_batch,
                        metadata={"stage": "written"},
                    )
                    self.assertIsNotNone(checkpoint)
                    assert checkpoint is not None
                    self.assertEqual(checkpoint.sequence, 1)
                    stored = await client.get_stream_checkpoint(
                        run_id="run_stream_api",
                        document_id="doc_stream",
                        process_id="extract",
                        stream_id="pages",
                        consumer_id="writer",
                    )
                    self.assertIsNotNone(stored)
                    assert stored is not None
                    self.assertEqual(stored.chunk_id, chunks[0].chunk_id)
                    self.assertEqual(stored.metadata["stage"], "written")
                    empty_batch = await client.read_stream_batch(
                        run_id="run_stream_api",
                        document_id="doc_stream",
                        process_id="extract",
                        stream_id="pages",
                        consumer_id="writer",
                    )
                    self.assertEqual(empty_batch.chunk_count, 0)
                    empty_commit = await client.commit_stream_batch(empty_batch)
                    self.assertIsNotNone(empty_commit)
                    assert empty_commit is not None
                    self.assertEqual(empty_commit.sequence, stored.sequence)

            asyncio.run(run_client())

    def test_runtime_api_validates_stream_chunk_contracts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            page_path = root / "page.txt"
            page_path.write_text("page text", encoding="utf-8")
            registry = PipelineRegistry()
            registry.add_package(
                WorkflowPackageSpec(
                    id="pkg",
                    pipelines=["typed.yaml"],
                    document_types=[DocumentTypeSpec(id="generic_document")],
                    artifact_kinds=[
                        ArtifactKindSpec(
                            id="page_text",
                            media_types=["text/plain"],
                            extensions=[".txt"],
                        ),
                        ArtifactKindSpec(id="image"),
                    ],
                    capabilities=[
                        CapabilitySpec(
                            id="extract_text",
                            accepts_document_types=["generic_document"],
                            emits_streams=[
                                StreamSpec(
                                    stream_id="pages",
                                    kinds=["page"],
                                    emits_artifact_kinds=["page_text"],
                                    value_schema={
                                        "type": "object",
                                        "required": ["text"],
                                        "properties": {"text": {"type": "string"}},
                                        "additionalProperties": True,
                                    },
                                    metadata_schema={
                                        "type": "object",
                                        "required": ["page_number"],
                                        "properties": {
                                            "page_number": {"type": "integer"}
                                        },
                                        "additionalProperties": True,
                                    },
                                )
                            ],
                        )
                    ],
                )
            )
            registry.add(
                PipelineSpec(
                    id="typed_flow",
                    steps=[
                        ProcessSpec(
                            id="extract",
                            capability="extract_text",
                            adapter=AdapterSpec(kind="queue", queue="typed.extract"),
                        )
                    ],
                ),
                package_id="pkg",
            )
            registry.validate_package_workers("pkg")
            store = InMemoryStateStore()
            service = RuntimeService(
                registry=registry,
                store=store,
                artifact_roots=[root],
                artifact_store_root=root / "artifact-store",
            )
            app = FastAPI()
            app.include_router(create_runtime_router(service), prefix="/api")

            async def run_client() -> None:
                await service.create_run_with_documents(
                    RuntimeRunInput(
                        run_id="run_stream_contract",
                        pipeline_id="typed_flow",
                        documents=[
                            RuntimeDocumentInput(
                                document_id="doc.txt",
                                document_type="generic_document",
                            )
                        ],
                    )
                )
                transport = httpx.ASGITransport(app=app)
                async with httpx.AsyncClient(
                    transport=transport,
                    base_url="http://runtime.test",
                ) as http_client:
                    client = ProcessRuntimeClient(
                        "http://runtime.test",
                        client=http_client,
                    )
                    valid = await client.append_stream_chunk(
                        run_id="run_stream_contract",
                        document_id="doc.txt",
                        process_id="extract",
                        stream_id="pages",
                        kind="page",
                        values={"text": "hello"},
                        metadata={"page_number": 1},
                        artifacts=[
                            ArtifactRef(
                                id="page_payload",
                                kind="page_text",
                                uri=page_path.as_uri(),
                                metadata={
                                    "media_type": "text/plain",
                                    "filename": "page.txt",
                                },
                            )
                        ],
                    )
                    self.assertEqual(valid.sequence, 0)
                    self.assertTrue(valid.artifacts[0].uri.startswith("fala-artifact://"))

                    bad_values = await http_client.post(
                        "/api/runs/run_stream_contract/process-runtime/"
                        "doc.txt/processes/extract/streams/pages/chunks",
                        json={
                            "kind": "page",
                            "values": {"text": 42},
                            "metadata": {"page_number": 2},
                        },
                    )
                    self.assertEqual(bad_values.status_code, 400, bad_values.text)
                    self.assertIn("stream 'pages' values", bad_values.json()["detail"])

                    bad_kind = await http_client.post(
                        "/api/runs/run_stream_contract/process-runtime/"
                        "doc.txt/processes/extract/streams/pages/chunks",
                        json={
                            "kind": "section",
                            "values": {"text": "hello"},
                            "metadata": {"page_number": 2},
                        },
                    )
                    self.assertEqual(bad_kind.status_code, 400, bad_kind.text)
                    self.assertIn("kind 'section'", bad_kind.json()["detail"])

                    bad_artifact = await http_client.post(
                        "/api/runs/run_stream_contract/process-runtime/"
                        "doc.txt/processes/extract/streams/pages/chunks",
                        json={
                            "kind": "page",
                            "values": {"text": "hello"},
                            "metadata": {"page_number": 2},
                            "artifacts": [
                                {
                                    "id": "preview",
                                    "kind": "image",
                                    "uri": "s3://bucket/preview.png",
                                }
                            ],
                        },
                    )
                    self.assertEqual(bad_artifact.status_code, 400, bad_artifact.text)
                    self.assertIn("not emitted by capability", bad_artifact.json()["detail"])

                chunks = await store.list_stream_chunks(
                    run_id="run_stream_contract",
                    document_id="doc.txt",
                    process_id="extract",
                    stream_id="pages",
                )
                self.assertEqual([chunk.sequence for chunk in chunks], [0])

            asyncio.run(run_client())

    def test_runtime_api_and_client_expose_retry_backoff_metrics(self) -> None:
        pipeline = PipelineSpec(
            id="backoff_metrics_flow",
            steps=[
                ProcessSpec(
                    id="ocr",
                    retry=RetryPolicy(max_attempts=2, delay_seconds=60),
                    adapter=AdapterSpec(kind="queue", queue="metrics.ocr"),
                ),
            ],
        )
        store = InMemoryStateStore()
        service = RuntimeService(
            registry=PipelineRegistry([pipeline]),
            store=store,
        )
        app = FastAPI()
        app.include_router(create_runtime_router(service), prefix="/api")

        async def run_client() -> None:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_backoff_metrics",
                    pipeline_id="backoff_metrics_flow",
                    documents=[RuntimeDocumentInput(document_id="doc_backoff")],
                )
            )
            claim = await service.claim_next(
                run_id="run_backoff_metrics",
                pipeline_id="backoff_metrics_flow",
                worker_id="worker-ocr",
                adapter_kind="queue",
            )
            self.assertIsNotNone(claim)
            await PipelineScheduler(pipeline, store).record_process_failure(
                run_id="run_backoff_metrics",
                document_id="doc_backoff",
                process_id="ocr",
                reason="temporary failure",
            )

            direct = await service.queue_metrics("run_backoff_metrics")
            self.assertEqual(direct.retry_backoff_count, 1)
            ocr = next(item for item in direct.processes if item.process_id == "ocr")
            self.assertEqual(ocr.retry_backoff_count, 1)
            self.assertEqual(ocr.next_retry_after_document_id, "doc_backoff")
            self.assertIsNotNone(ocr.next_retry_after)

            async with ProcessRuntimeClient(
                "http://runtime.test",
                transport=httpx.ASGITransport(app=app),
            ) as client:
                metrics = await client.get_queue_metrics(run_id="run_backoff_metrics")
                self.assertEqual(metrics.retry_backoff_count, 1)
                process = metrics.processes[0]
                self.assertEqual(process.process_id, "ocr")
                self.assertEqual(process.retry_backoff_count, 1)
                self.assertEqual(process.next_retry_after_document_id, "doc_backoff")

        asyncio.run(run_client())

    def test_runtime_run_pause_resume_blocks_new_claims(self) -> None:
        pipeline = PipelineSpec(
            id="pause_flow",
            steps=[
                ProcessSpec(
                    id="extract",
                    adapter=AdapterSpec(kind="queue", queue="pause.extract"),
                )
            ],
        )
        store = InMemoryStateStore()
        service = RuntimeService(
            registry=PipelineRegistry([pipeline]),
            store=store,
        )
        app = FastAPI()
        app.include_router(create_runtime_router(service), prefix="/api")

        async def run_client() -> None:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_pause",
                    pipeline_id="pause_flow",
                    documents=[RuntimeDocumentInput(document_id="doc_pause")],
                )
            )

            async with ProcessRuntimeClient(
                "http://runtime.test",
                transport=httpx.ASGITransport(app=app),
            ) as client:
                paused = await client.control_run(
                    run_id="run_pause",
                    action="pause",
                    reason="operator pause",
                )
                self.assertEqual(paused["run"]["status"], "paused")
                self.assertIsNone(
                    await client.claim_next(
                        run_id="run_pause",
                        pipeline_id="pause_flow",
                        worker_id="worker-a",
                        adapter_kind="queue",
                    )
                )
                summaries = await service.list_run_summaries(limit=10)
                paused_summary = next(item for item in summaries if item["run_id"] == "run_pause")
                self.assertEqual(paused_summary["status"], "paused")

                resumed = await client.control_run(
                    run_id="run_pause",
                    action="resume",
                    reason="operator resume",
                )
                self.assertEqual(resumed["run"]["status"], "queued")
                claim = await client.claim_next(
                    run_id="run_pause",
                    pipeline_id="pause_flow",
                    worker_id="worker-a",
                    adapter_kind="queue",
                )
                self.assertIsNotNone(claim)
                assert claim is not None
                self.assertEqual(claim.document_id, "doc_pause")
                self.assertEqual(claim.process.id, "extract")

                cancelled = await client.control_run(
                    run_id="run_pause",
                    action="cancel",
                    reason="operator cancel",
                )
                self.assertEqual(cancelled["run"]["status"], "cancelled")
                self.assertEqual(cancelled["run"]["outcome"], "cancelled")
                self.assertEqual(
                    cancelled["run"]["metadata"]["cancel_reason"],
                    "operator cancel",
                )
                self.assertIsNone(
                    await store.get_claim(
                        run_id="run_pause",
                        document_id="doc_pause",
                        process_id="extract",
                    )
                )
                self.assertIsNone(
                    await client.claim_next(
                        run_id="run_pause",
                        pipeline_id="pause_flow",
                        worker_id="worker-a",
                        adapter_kind="queue",
                    )
                )
                statuses = await store.list_statuses(
                    run_id="run_pause",
                    document_id="doc_pause",
                )
                self.assertEqual(statuses["extract"], ProcessStatus.cancelled)
                with self.assertRaisesRegex(ValueError, "terminal"):
                    await service.put_process_output(
                        run_id="run_pause",
                        document_id="doc_pause",
                        process_id="extract",
                        output=ProcessOutput(values={"late": True}),
                        pipeline_id="pause_flow",
                    )
                with self.assertRaisesRegex(ValueError, "terminal"):
                    await service.append_stream_chunk(
                        run_id="run_pause",
                        document_id="doc_pause",
                        process_id="extract",
                        values={"late": True},
                    )

        asyncio.run(run_client())

    def test_runtime_api_and_client_expose_worker_health(self) -> None:
        pipeline = PipelineSpec(
            id="worker_health_flow",
            steps=[
                ProcessSpec(
                    id="extract",
                    adapter=AdapterSpec(kind="queue", queue="health.extract"),
                )
            ],
        )
        store = InMemoryStateStore()
        service = RuntimeService(
            registry=PipelineRegistry([pipeline]),
            store=store,
        )
        app = FastAPI()
        app.include_router(create_runtime_router(service), prefix="/api")

        async def run_client() -> None:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_worker_health",
                    pipeline_id="worker_health_flow",
                    documents=[RuntimeDocumentInput(document_id="doc_worker_health")],
                )
            )

            async with ProcessRuntimeClient(
                "http://runtime.test",
                transport=httpx.ASGITransport(app=app),
            ) as client:
                heartbeat = await client.worker_heartbeat(
                    run_id="run_worker_health",
                    worker_id="worker-health",
                    pipeline_id="worker_health_flow",
                    adapter_kind="queue",
                    capabilities=["extract"],
                    status=RuntimeWorkerStatus.working,
                    current_document_id="doc_worker_health",
                    current_process_id="extract",
                )
                self.assertEqual(heartbeat["worker_id"], "worker-health")
                self.assertEqual(heartbeat["status"], "working")

                workers = await client.worker_health(run_id="run_worker_health")
                self.assertEqual(len(workers), 1)
                self.assertEqual(workers[0].worker_id, "worker-health")
                self.assertTrue(workers[0].healthy)
                self.assertEqual(workers[0].current_process_id, "extract")

                await asyncio.sleep(0.001)
                stale = await client.worker_health(
                    run_id="run_worker_health",
                    stale_after_seconds=0.000001,
                )
                self.assertFalse(stale[0].healthy)

        asyncio.run(run_client())

    def test_runtime_api_and_client_expose_process_trace(self) -> None:
        pipeline = PipelineSpec(
            id="trace_flow",
            steps=[
                ProcessSpec(
                    id="extract",
                    capability="extract_text",
                    retry=RetryPolicy(max_attempts=2),
                    adapter=AdapterSpec(kind="queue", queue="trace.extract"),
                )
            ],
        )
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="trace_pkg",
                pipelines=["trace.yaml"],
                operation_types=[OperationTypeSpec(id="extract")],
                capabilities=[
                    CapabilitySpec(id="extract_text", operation_type="extract")
                ],
            )
        )
        registry.add(pipeline, package_id="trace_pkg")
        store = InMemoryStateStore()
        service = RuntimeService(
            registry=registry,
            store=store,
        )
        app = FastAPI()
        app.include_router(create_runtime_router(service), prefix="/api")

        async def run_client() -> None:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_trace",
                    pipeline_id="trace_flow",
                    documents=[RuntimeDocumentInput(document_id="doc_trace")],
                )
            )
            first = await service.claim_next(
                run_id="run_trace",
                pipeline_id="trace_flow",
                worker_id="worker-a",
                adapter_kind="queue",
            )
            self.assertIsNotNone(first)
            assert first is not None
            await PipelineScheduler(pipeline, store).record_process_failure(
                run_id="run_trace",
                document_id="doc_trace",
                process_id="extract",
                reason="retry me",
            )
            second = await service.claim_next(
                run_id="run_trace",
                pipeline_id="trace_flow",
                worker_id="worker-b",
                adapter_kind="queue",
            )
            self.assertIsNotNone(second)
            assert second is not None
            await service.store.append_event(
                ProcessEvent(
                    run_id="run_trace",
                    document_id="doc_trace",
                    process_id="extract",
                    type="process.progress",
                    status=ProcessStatus.running,
                    data={"attempt": 2, "stage": "running"},
                )
            )

            direct = await service.process_trace(
                "run_trace",
                document_id="doc_trace",
                process_id="extract",
                operation_type="extract",
            )
            self.assertEqual(direct.operation_type, "extract")
            self.assertEqual(direct.process_count, 1)
            self.assertEqual(direct.attempt_count, 2)
            process = direct.processes[0]
            self.assertEqual(process.document_id, "doc_trace")
            self.assertEqual(process.process_id, "extract")
            self.assertEqual(process.operation_type, "extract")
            self.assertEqual([attempt.attempt for attempt in process.attempts], [None, 1, 2])
            attempt_1 = next(attempt for attempt in process.attempts if attempt.attempt == 1)
            self.assertEqual(attempt_1.worker_id, "worker-a")
            self.assertIn("process.claimed", attempt_1.event_types)
            self.assertIn("process.retry_scheduled", attempt_1.event_types)
            self.assertTrue(
                all(
                    event.operation_type == "extract"
                    for event in attempt_1.events
                    if event.process_id == "extract"
                )
            )
            attempt_2 = next(attempt for attempt in process.attempts if attempt.attempt == 2)
            self.assertEqual(attempt_2.worker_id, "worker-b")
            self.assertIn("process.progress", attempt_2.event_types)

            async with ProcessRuntimeClient(
                "http://runtime.test",
                transport=httpx.ASGITransport(app=app),
            ) as client:
                trace = await client.get_trace(
                    run_id="run_trace",
                    document_id="doc_trace",
                    process_id="extract",
                    operation_type="extract",
                )
                self.assertEqual(trace.run_id, "run_trace")
                self.assertEqual(trace.operation_type, "extract")
                self.assertEqual(trace.process_count, 1)
                self.assertEqual(trace.attempt_count, 2)
                self.assertEqual(trace.processes[0].operation_type, "extract")
                self.assertEqual(trace.processes[0].attempts[-1].worker_id, "worker-b")
                empty_trace = await client.get_trace(
                    run_id="run_trace",
                    operation_type="render",
                )
                self.assertEqual(empty_trace.process_count, 0)
                event_page = await client.list_events(
                    run_id="run_trace",
                    document_id="doc_trace",
                    operation_type="extract",
                    limit=20,
                )
                self.assertGreater(event_page.count, 0)
                self.assertEqual(event_page.operation_type, "extract")
                self.assertTrue(
                    all(event.operation_type == "extract" for event in event_page.events)
                )
                empty_events = await client.list_events(
                    run_id="run_trace",
                    document_id="doc_trace",
                    operation_type="render",
                    limit=20,
                )
                self.assertEqual(empty_events.count, 0)
                streamed_events: list[ProcessEvent] = []
                async for event in client.stream_events(
                    run_id="run_trace",
                    operation_type="extract",
                    max_events=1,
                    poll_interval_seconds=0.05,
                    heartbeat_interval_seconds=1.0,
                ):
                    streamed_events.append(event)
                self.assertEqual(len(streamed_events), 1)
                self.assertEqual(streamed_events[0].operation_type, "extract")

        asyncio.run(run_client())

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
            if request.url.path.endswith("/process-runtime/report"):
                step = {
                    "run_id": "run_http",
                    "document_id": "folder/doc.pdf",
                    "pipeline_id": "pipeline",
                    "process_id": "extract",
                    "position": 0,
                    "status": "running",
                    "status_category": "running",
                    "is_active": True,
                    "has_claim": True,
                    "worker_id": "worker-http",
                }
                return httpx.Response(
                    200,
                    json={
                        "ok": True,
                        "run_id": "run_http",
                        "summary": {
                            "document_count": 1,
                            "process_count": 1,
                            "active_process_count": 1,
                            "status_counts": {"running": 1},
                            "pipeline_counts": {"pipeline": 1},
                        },
                        "documents": [
                            {
                                "run_id": "run_http",
                                "document_id": "folder/doc.pdf",
                                "pipeline_id": "pipeline",
                                "process_count": 1,
                                "active_process_count": 1,
                                "status_counts": {"running": 1},
                                "steps": [step],
                            }
                        ],
                        "steps": [step],
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
                report = await client.get_step_report(run_id="run_http")
                self.assertEqual(report.summary.process_count, 1)
                self.assertEqual(report.steps[0].worker_id, "worker-http")
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
            "/api/runs/run_http/process-runtime/report",
        )
        self.assertEqual(
            requests[6].url.raw_path.decode("utf-8"),
            "/api/runs/run_http/process-runtime/attach",
        )
        self.assertEqual(json.loads(requests[6].content)["pipeline_id"], "pipeline")
        self.assertEqual(
            requests[7].url.raw_path.decode("utf-8"),
            "/api/runs/run_http/process-runtime/claim",
        )
        self.assertEqual(json.loads(requests[7].content)["capabilities"], [])
        self.assertEqual(
            requests[8].url.raw_path.decode("utf-8").split("?", 1)[0],
            "/api/runs/run_http/process-runtime/folder%2Fdoc.pdf/processes/extract/output",
        )
        self.assertEqual(requests[8].url.params["pipeline_id"], "pipeline")
        self.assertEqual(requests[8].url.params["worker_id"], "worker-http")
        self.assertEqual(
            requests[9].url.raw_path.decode("utf-8"),
            "/api/runs/run_http/process-runtime/folder%2Fdoc.pdf/schedule",
        )
        self.assertEqual(
            requests[10].url.raw_path.decode("utf-8"),
            "/api/runs/run_http/process-runtime/folder%2Fdoc.pdf/processes/extract/actions",
        )
        action_payload = json.loads(requests[10].content)
        self.assertEqual(action_payload["action"], "retry")
        self.assertEqual(action_payload["pipeline_id"], "pipeline")
        self.assertEqual(action_payload["reason"], "operator retry")
        self.assertEqual(
            requests[11].url.raw_path.decode("utf-8").split("?", 1)[0],
            "/api/runs/run_http/process-runtime/folder%2Fdoc.pdf/events",
        )
        self.assertEqual(requests[11].url.params["process_id"], "extract")
        self.assertEqual(requests[11].url.params["after_event_id"], "event_previous")
        self.assertEqual(requests[11].url.params["limit"], "10")

    def test_runtime_client_streams_process_events(self) -> None:
        store = InMemoryStateStore()
        service = RuntimeService(registry=PipelineRegistry(), store=store)
        app = FastAPI()
        app.include_router(create_runtime_router(service), prefix="/api")
        first = ProcessEvent(
            id="event_first",
            run_id="run_stream",
            document_id="doc_a",
            process_id="extract",
            type="process.started",
            status=ProcessStatus.running,
            data={"step": 1},
        )
        second = ProcessEvent(
            id="event_second",
            run_id="run_stream",
            document_id="doc_b",
            process_id="render",
            type="process.completed",
            status=ProcessStatus.completed,
            data={"step": 2},
        )
        asyncio.run(store.append_event(first))
        asyncio.run(store.append_event(second))

        async def collect() -> tuple[list[ProcessEvent], list[ProcessEvent]]:
            async with ProcessRuntimeClient(
                "http://runtime.test",
                transport=httpx.ASGITransport(app=app),
            ) as client:
                run_events: list[ProcessEvent] = []
                async for event in client.stream_events(
                    run_id="run_stream",
                    after_event_id=first.id,
                    max_events=1,
                    poll_interval_seconds=0.05,
                    heartbeat_interval_seconds=1.0,
                ):
                    run_events.append(event)

                document_events: list[ProcessEvent] = []
                async for event in client.stream_events(
                    run_id="run_stream",
                    document_id="doc_a",
                    max_events=1,
                    poll_interval_seconds=0.05,
                    heartbeat_interval_seconds=1.0,
                ):
                    document_events.append(event)
                return run_events, document_events

        run_events, document_events = asyncio.run(collect())

        self.assertEqual([event.id for event in run_events], ["event_second"])
        self.assertEqual([event.id for event in document_events], ["event_first"])
        self.assertEqual(run_events[0].data["step"], 2)

    def test_file_artifact_store_uses_content_addressed_refs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "payload.txt"
            source.write_text("hello artifact", encoding="utf-8")
            store = FileArtifactStore(root / "store")

            ref = store.put_file(kind="text", path=source, artifact_id="payload")
            resolved = store.resolve(ref)

            self.assertEqual(ref.id, "payload")
            self.assertEqual(ref.kind, "text")
            self.assertTrue(ref.uri.startswith("fala-artifact://sha256/"))
            self.assertEqual(ref.metadata["filename"], "payload.txt")
            self.assertEqual(ref.metadata["size_bytes"], source.stat().st_size)
            self.assertEqual(resolved.read_text(encoding="utf-8"), "hello artifact")

    def test_memory_artifact_store_implements_content_addressed_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "payload.json"
            source.write_text(json.dumps({"ok": True}), encoding="utf-8")
            store = MemoryArtifactStore("memory://test-store")

            ref = store.put_file(kind="json", path=source, artifact_id="payload")
            resolved = store.resolve(ref)
            with store.open(ref) as handle:
                stored = handle.read()

            self.assertEqual(ref.id, "payload")
            self.assertTrue(ref.uri.startswith("fala-artifact://sha256/"))
            self.assertEqual(ref.metadata["storage"]["backend"], "memory")
            self.assertEqual(stored, source.read_bytes())
            self.assertEqual(resolved.read_bytes(), source.read_bytes())
            blobs = store.list_blobs()
            self.assertEqual(len(blobs), 1)
            self.assertEqual(blobs[0].location, f"memory://test-store/sha256/{blobs[0].digest}")
            self.assertEqual(store.delete_blobs([blobs[0].digest]), [blobs[0].digest])
            with self.assertRaises(FileNotFoundError):
                store.open(ref)

    def test_s3_artifact_store_implements_content_addressed_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "payload.txt"
            source.write_text("hello s3 artifact", encoding="utf-8")
            client = _FakeS3Client()
            store = S3ArtifactStore(
                "s3://fala-test/prefix",
                client=client,
                materialized_root=root / "materialized",
            )

            ref = store.put_file(kind="text", path=source, artifact_id="payload")
            duplicate = store.put_fileobj(
                kind="text",
                fileobj=BytesIO(source.read_bytes()),
                filename="payload-copy.txt",
            )
            with store.open(ref) as handle:
                stored = handle.read()
            resolved = store.resolve(ref)
            blobs = store.list_blobs()

            self.assertEqual(ref.id, "payload")
            self.assertTrue(ref.uri.startswith("fala-artifact://sha256/"))
            self.assertEqual(duplicate.uri, ref.uri)
            self.assertEqual(client.put_count, 1)
            self.assertEqual(stored, source.read_bytes())
            self.assertEqual(resolved.read_bytes(), source.read_bytes())
            self.assertEqual(ref.metadata["storage"]["backend"], "s3")
            self.assertEqual(ref.metadata["storage"]["bucket"], "fala-test")
            self.assertTrue(ref.metadata["storage"]["key"].startswith("prefix/blobs/sha256/"))
            self.assertEqual(len(blobs), 1)
            self.assertEqual(blobs[0].location, f"s3://fala-test/{ref.metadata['storage']['key']}")
            self.assertEqual(store.delete_blobs([blobs[0].digest]), [blobs[0].digest])
            self.assertEqual(store.list_blobs(), [])
            with self.assertRaises(FileNotFoundError):
                store.open(ref)

    def test_create_artifact_store_uses_env_backend_selector(self) -> None:
        old_store = os.environ.get("FALA_ARTIFACT_STORE")
        old_root = os.environ.get("FALA_ARTIFACT_STORE_ROOT")
        try:
            os.environ["FALA_ARTIFACT_STORE"] = "memory://env-store"
            os.environ["FALA_ARTIFACT_STORE_ROOT"] = "/ignored"
            store = create_artifact_store()
            self.assertIsInstance(store, MemoryArtifactStore)
            self.assertEqual(store.location, "memory://env-store")
        finally:
            if old_store is None:
                os.environ.pop("FALA_ARTIFACT_STORE", None)
            else:
                os.environ["FALA_ARTIFACT_STORE"] = old_store
            if old_root is None:
                os.environ.pop("FALA_ARTIFACT_STORE_ROOT", None)
            else:
                os.environ["FALA_ARTIFACT_STORE_ROOT"] = old_root

    def test_runtime_service_can_materialize_and_gc_memory_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            artifact_root = root / "runtime-artifacts"
            artifact_root.mkdir()
            output_path = artifact_root / "output.json"
            orphan_path = artifact_root / "orphan.json"
            output_path.write_text(json.dumps({"ok": True}), encoding="utf-8")
            orphan_path.write_text(json.dumps({"unused": True}), encoding="utf-8")
            store = MemoryArtifactStore("memory://runtime-store")
            pipeline = PipelineSpec(
                id="memory_artifact_flow",
                steps=[
                    ProcessSpec(
                        id="extract",
                        capability="emit_json",
                        adapter=AdapterSpec(kind="queue", queue="memory.extract"),
                    )
                ],
            )
            package = WorkflowPackageSpec(
                id="memory_artifact_package",
                artifact_kinds=[
                    ArtifactKindSpec(
                        id="json_payload",
                        value_schema={
                            "type": "object",
                            "properties": {"ok": {"type": "boolean"}},
                            "required": ["ok"],
                        },
                    )
                ],
                capabilities=[
                    CapabilitySpec(
                        id="emit_json",
                        emits_artifact_kinds=["json_payload"],
                    )
                ],
                pipelines=["memory_artifact_flow.yaml"],
            )
            registry = PipelineRegistry()
            registry.add_package(package)
            registry.add(pipeline, package_id=package.id)
            service = RuntimeService(
                registry=registry,
                store=InMemoryStateStore(),
                artifact_roots=[artifact_root],
                artifact_store=store,
            )
            orphan = store.put_file(kind="json_payload", path=orphan_path)

            async def exercise() -> tuple[ProcessOutput, Any, Any]:
                await service.create_run_with_documents(
                    RuntimeRunInput(
                        run_id="run_memory_artifacts",
                        pipeline_id="memory_artifact_flow",
                        documents=[RuntimeDocumentInput(document_id="doc.json")],
                    )
                )
                output = await service.put_process_output(
                    run_id="run_memory_artifacts",
                    document_id="doc.json",
                    process_id="extract",
                    pipeline_id="memory_artifact_flow",
                    output=ProcessOutput(
                        artifacts=[
                            ArtifactRef(
                                id="payload",
                                kind="json_payload",
                                uri=output_path.as_uri(),
                            )
                        ]
                    ),
                )
                dry_run = await service.artifact_gc(dry_run=True)
                deleted = await service.artifact_gc(dry_run=False)
                return output, dry_run, deleted

            output, dry_run, deleted = asyncio.run(exercise())

            self.assertEqual(output.artifacts[0].metadata["storage"]["backend"], "memory")
            self.assertTrue(output.artifacts[0].uri.startswith("fala-artifact://sha256/"))
            with store.open(output.artifacts[0]) as handle:
                self.assertEqual(json.loads(handle.read().decode("utf-8")), {"ok": True})
            self.assertEqual(dry_run.root, "memory://runtime-store")
            self.assertEqual(dry_run.blob_count, 2)
            self.assertEqual(dry_run.referenced_blob_count, 1)
            self.assertEqual(dry_run.orphaned_blob_count, 1)
            self.assertEqual(dry_run.orphaned_blobs[0].uri, orphan.uri)
            self.assertEqual(deleted.deleted_blob_count, 1)
            with self.assertRaises(FileNotFoundError):
                store.open(orphan)

    def test_runtime_artifact_gc_reports_and_deletes_orphaned_blobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipeline = PipelineSpec(
                id="artifact_gc_flow",
                steps=[
                    ProcessSpec(
                        id="extract",
                        adapter=AdapterSpec(kind="queue", queue="gc.extract"),
                    )
                ],
            )
            service = RuntimeService(
                registry=PipelineRegistry([pipeline]),
                store=InMemoryStateStore(),
                artifact_store_root=root / "artifact-store",
            )

            source_path = root / "source.pdf"
            output_path = root / "output.json"
            orphan_path = root / "orphan.bin"
            source_path.write_bytes(b"source")
            output_path.write_bytes(b"output")
            orphan_path.write_bytes(b"orphan")
            source_ref = service.artifact_store.put_file(
                kind="source_document",
                path=source_path,
            )
            output_ref = service.artifact_store.put_file(
                kind="extracted_payload",
                path=output_path,
            )
            orphan_ref = service.artifact_store.put_file(
                kind="unused_payload",
                path=orphan_path,
            )

            async def run_check() -> None:
                await service.create_run_with_documents(
                    RuntimeRunInput(
                        run_id="run_artifact_gc",
                        pipeline_id="artifact_gc_flow",
                        documents=[
                            RuntimeDocumentInput(
                                document_id="doc.pdf",
                                source_uri=source_ref.uri,
                            )
                        ],
                    )
                )
                await service.put_process_output(
                    run_id="run_artifact_gc",
                    document_id="doc.pdf",
                    process_id="extract",
                    output=ProcessOutput(artifacts=[output_ref]),
                    pipeline_id="artifact_gc_flow",
                )

                dry_run = await service.artifact_gc(dry_run=True)
                self.assertTrue(dry_run.dry_run)
                self.assertEqual(dry_run.blob_count, 3)
                self.assertEqual(dry_run.referenced_blob_count, 2)
                self.assertEqual(dry_run.orphaned_blob_count, 1)
                self.assertEqual(dry_run.deleted_blob_count, 0)
                self.assertEqual(dry_run.orphaned_blobs[0].uri, orphan_ref.uri)
                self.assertTrue(service.artifact_store.resolve(orphan_ref).exists())

                app = FastAPI()
                app.include_router(create_runtime_router(service), prefix="/api")
                async with ProcessRuntimeClient(
                    "http://runtime.test",
                    transport=httpx.ASGITransport(app=app),
                ) as client:
                    client_dry_run = await client.artifact_gc()
                    self.assertTrue(client_dry_run["dry_run"])
                    self.assertEqual(client_dry_run["orphaned_blob_count"], 1)

                    deleted = await client.artifact_gc(delete=True)
                    self.assertFalse(deleted["dry_run"])
                    self.assertEqual(deleted["deleted_blob_count"], 1)
                    self.assertEqual(deleted["orphaned_blobs"][0]["uri"], orphan_ref.uri)
                    self.assertTrue(deleted["orphaned_blobs"][0]["deleted"])

                self.assertTrue(service.artifact_store.resolve(source_ref).exists())
                self.assertTrue(service.artifact_store.resolve(output_ref).exists())
                with self.assertRaises(FileNotFoundError):
                    service.artifact_store.resolve(orphan_ref)

                clean = await service.artifact_gc(dry_run=True)
                self.assertEqual(clean.blob_count, 2)
                self.assertEqual(clean.orphaned_blob_count, 0)

            asyncio.run(run_check())

    def test_runtime_api_materializes_local_output_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            artifact_root = root / "runtime-artifacts"
            artifact_root.mkdir()
            source = artifact_root / "payload.json"
            source.write_text(json.dumps({"ok": True}), encoding="utf-8")
            pipeline = PipelineSpec(
                id="artifact_flow",
                steps=[
                    ProcessSpec(
                        id="extract",
                        adapter=AdapterSpec(kind="queue", queue="artifact.extract"),
                    )
                ],
            )
            service = RuntimeService(
                registry=PipelineRegistry([pipeline]),
                store=InMemoryStateStore(),
                artifact_roots=[artifact_root],
                artifact_store_root=root / "artifact-store",
            )
            app = FastAPI()
            app.include_router(create_runtime_router(service), prefix="/api")

            async def exercise() -> tuple[ProcessOutput, bytes]:
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://runtime.test",
                ) as client:
                    init = await client.post(
                        "/api/runs/run_artifacts/process-runtime/documents",
                        json={
                            "pipeline_id": "artifact_flow",
                            "document_id": "doc.json",
                            "values": {},
                            "artifacts": [],
                        },
                    )
                    self.assertEqual(init.status_code, 200, init.text)
                    write = await client.put(
                        "/api/runs/run_artifacts/process-runtime/"
                        "doc.json/processes/extract/output",
                        params={"pipeline_id": "artifact_flow"},
                        json=ProcessOutput(
                            values={"ok": True},
                            artifacts=[
                                ArtifactRef(
                                    id="payload",
                                    kind="json",
                                    uri=source.as_uri(),
                                )
                            ],
                        ).model_dump(mode="json"),
                    )
                    self.assertEqual(write.status_code, 200, write.text)
                    stored = await service.store.get_output(
                        run_id="run_artifacts",
                        document_id="doc.json",
                        process_id="extract",
                    )
                    self.assertIsNotNone(stored)
                    assert stored is not None
                    download = await client.get(
                        "/api/runs/run_artifacts/process-runtime/"
                        "doc.json/processes/extract/artifacts/payload/download"
                    )
                    self.assertEqual(download.status_code, 200, download.text)
                    return stored, download.content

            stored, downloaded = asyncio.run(exercise())

            stored_ref = stored.artifacts[0]
            self.assertEqual(stored_ref.id, "payload")
            self.assertTrue(stored_ref.uri.startswith("fala-artifact://sha256/"))
            self.assertEqual(stored_ref.metadata["filename"], "payload.json")
            self.assertEqual(downloaded, source.read_bytes())
            lineage = stored.metadata["process_runtime"]["lineage"]
            self.assertEqual(lineage["schema_version"], 1)
            self.assertEqual(lineage["pipeline_id"], "artifact_flow")
            self.assertEqual(lineage["document_id"], "doc.json")
            self.assertEqual(lineage["process_id"], "extract")
            self.assertEqual(lineage["needs"], [])
            self.assertEqual(lineage["input_artifact_count"], 0)
            self.assertEqual(lineage["initial_value_keys"], [])

    def test_runtime_process_output_records_input_and_dependency_lineage(self) -> None:
        pipeline = PipelineSpec(
            id="lineage_flow",
            steps=[
                ProcessSpec(
                    id="extract",
                    adapter=AdapterSpec(kind="queue", queue="lineage.extract"),
                ),
                ProcessSpec(
                    id="classify",
                    needs=["extract"],
                    adapter=AdapterSpec(kind="queue", queue="lineage.classify"),
                ),
            ],
        )
        service = RuntimeService(
            registry=PipelineRegistry([pipeline]),
            store=InMemoryStateStore(),
        )

        async def exercise() -> ProcessOutput:
            await service.initialize_document(
                run_id="run_lineage",
                document_id="doc.pdf",
                pipeline_id="lineage_flow",
                values={"case_id": "C-1"},
                artifacts=[
                    ArtifactRef(
                        id="source_doc",
                        kind="source_document",
                        uri="file:///tmp/doc.pdf",
                    )
                ],
            )
            await service.put_process_output(
                run_id="run_lineage",
                document_id="doc.pdf",
                process_id="extract",
                output=ProcessOutput(
                    values={"text": "hello"},
                    artifacts=[
                        ArtifactRef(
                            id="text_artifact",
                            kind="extracted_text",
                            uri="memory://text",
                            metadata={"sha256": "abc123"},
                        )
                    ],
                ),
                pipeline_id="lineage_flow",
                worker_id="extract-worker",
            )
            return await service.put_process_output(
                run_id="run_lineage",
                document_id="doc.pdf",
                process_id="classify",
                output=ProcessOutput(values={"label": "ok"}),
                pipeline_id="lineage_flow",
                worker_id="classify-worker",
            )

        classified = asyncio.run(exercise())
        lineage = classified.metadata["process_runtime"]["lineage"]
        self.assertEqual(lineage["worker_id"], "classify-worker")
        self.assertEqual(lineage["needs"], ["extract"])
        self.assertEqual(lineage["initial_value_keys"], ["case_id"])
        self.assertEqual(lineage["needs_value_keys"], {"extract": ["text"]})
        self.assertEqual(lineage["input_artifact_count"], 2)
        self.assertEqual(
            [artifact["id"] for artifact in lineage["input_artifacts"]],
            ["source_doc", "text_artifact"],
        )
        self.assertEqual(lineage["dependency_outputs"][0]["process_id"], "extract")
        self.assertEqual(lineage["dependency_outputs"][0]["value_keys"], ["text"])
        self.assertEqual(
            lineage["dependency_outputs"][0]["artifacts"][0]["id"],
            "text_artifact",
        )
        self.assertEqual(
            lineage["dependency_outputs"][0]["artifacts"][0]["sha256"],
            "abc123",
        )

    def test_runtime_manual_process_waits_for_operator_and_unblocks_downstream(self) -> None:
        pipeline = PipelineSpec(
            id="manual_flow",
            steps=[
                ProcessSpec(
                    id="review",
                    title="Human review",
                    adapter=AdapterSpec(kind="manual"),
                ),
                ProcessSpec(
                    id="export",
                    needs=["review"],
                    adapter=AdapterSpec(kind="queue", queue="manual.export"),
                ),
            ],
        )
        service = RuntimeService(
            registry=PipelineRegistry([pipeline]),
            store=InMemoryStateStore(),
        )

        async def exercise() -> tuple[dict, ProcessOutput, dict, ClaimedProcess | None]:
            plan = service.plan_runtime_run_input(
                RuntimeRunInput(
                    run_id="run_manual",
                    pipeline_id="manual_flow",
                    documents=[RuntimeDocumentInput(document_id="doc.pdf")],
                )
            )
            schedule = await service.initialize_document(
                run_id="run_manual",
                document_id="doc.pdf",
                pipeline_id="manual_flow",
            )
            self.assertEqual(schedule.queued, [])
            self.assertEqual(schedule.waiting, ["review", "export"])
            worker_claim = await service.claim_next(
                run_id="run_manual",
                pipeline_id="manual_flow",
                worker_id="queue-worker",
                adapter_kind="queue",
            )
            self.assertIsNone(worker_claim)
            output, _refreshed, next_schedule, _spawned = await service.complete_process_output(
                run_id="run_manual",
                document_id="doc.pdf",
                process_id="review",
                output=ProcessOutput(values={"approved": "yes"}),
                pipeline_id="manual_flow",
            )
            next_claim = await service.claim_next(
                run_id="run_manual",
                pipeline_id="manual_flow",
                worker_id="queue-worker",
                adapter_kind="queue",
            )
            return (
                plan,
                output,
                next_schedule.model_dump(mode="json"),
                next_claim,
            )

        plan, output, next_schedule, next_claim = asyncio.run(exercise())

        manual_process = next(
            process
            for process in plan["plan"]["processes"]
            if process["process_id"] == "review"
        )
        self.assertTrue(manual_process["manual_required"])
        self.assertEqual(manual_process["queued_count"], 0)
        self.assertEqual(manual_process["waiting_count"], 1)
        self.assertEqual(plan["plan"]["worker_demand_count"], 1)
        self.assertEqual(output.values, {"approved": "yes"})
        self.assertEqual(
            output.metadata["process_runtime"]["lineage"]["process_id"],
            "review",
        )
        self.assertEqual(next_schedule["completed"], ["review"])
        self.assertEqual([item["id"] for item in next_schedule["queued"]], ["export"])
        self.assertIsNotNone(next_claim)
        assert next_claim is not None
        self.assertEqual(next_claim.process.id, "export")

        async def read_events() -> list[str]:
            return [
                event.type
                for event in await service.store.list_events(
                    run_id="run_manual",
                    document_id="doc.pdf",
                )
            ]

        self.assertIn("process.manual_required", asyncio.run(read_events()))

    def test_runtime_process_conditions_route_heterogeneous_documents(self) -> None:
        pipeline = PipelineSpec(
            id="routing_flow",
            steps=[
                ProcessSpec(
                    id="parse_pdf",
                    when=ProcessConditionSpec(document_types=["pdf_document"]),
                    adapter=AdapterSpec(kind="queue", queue="routing.pdf"),
                ),
                ProcessSpec(
                    id="parse_email",
                    when=ProcessConditionSpec(
                        document_types=["email_document"],
                        media_types=["message/*"],
                    ),
                    adapter=AdapterSpec(kind="queue", queue="routing.email"),
                ),
            ],
        )
        service = RuntimeService(
            registry=PipelineRegistry([pipeline]),
            store=InMemoryStateStore(),
        )
        run_input = RuntimeRunInput(
            run_id="run_routing",
            pipeline_id="routing_flow",
            documents=[
                RuntimeDocumentInput(
                    document_id="case.pdf",
                    document_type="pdf_document",
                    media_type="application/pdf",
                    values={"tenant": "a"},
                ),
                RuntimeDocumentInput(
                    document_id="mail.eml",
                    document_type="email_document",
                    media_type="message/rfc822",
                    values={"tenant": "a"},
                ),
            ],
        )

        async def exercise() -> tuple[dict, list[dict], list[ClaimedProcess]]:
            plan = service.plan_runtime_run_input(run_input)
            _run, schedules = await service.create_run_with_documents(run_input)
            claims: list[ClaimedProcess] = []
            for _ in range(2):
                claim = await service.claim_next(
                    run_id="run_routing",
                    pipeline_id="routing_flow",
                    worker_id="router-worker",
                    adapter_kind="queue",
                )
                self.assertIsNotNone(claim)
                assert claim is not None
                claims.append(claim)
            return (
                plan,
                [schedule.model_dump(mode="json") for schedule in schedules],
                claims,
            )

        plan, schedules, claims = asyncio.run(exercise())

        self.assertEqual(plan["plan"]["queued_count"], 2)
        self.assertEqual(plan["plan"]["waiting_count"], 0)
        self.assertEqual(plan["plan"]["skipped_count"], 2)
        pdf_document = next(
            item
            for item in plan["plan"]["documents"]
            if item["document_id"] == "case.pdf"
        )
        self.assertEqual(
            [item["process_id"] for item in pdf_document["queued"]],
            ["parse_pdf"],
        )
        self.assertEqual(
            [item["process_id"] for item in pdf_document["skipped"]],
            ["parse_email"],
        )
        self.assertEqual(
            [item["id"] for item in schedules[0]["queued"]],
            ["parse_pdf"],
        )
        self.assertEqual(schedules[0]["skipped"], ["parse_email"])
        self.assertEqual(
            [item["id"] for item in schedules[1]["queued"]],
            ["parse_email"],
        )
        self.assertEqual(schedules[1]["skipped"], ["parse_pdf"])
        self.assertEqual(
            sorted((claim.document_id, claim.process.id) for claim in claims),
            [("case.pdf", "parse_pdf"), ("mail.eml", "parse_email")],
        )

        async def read_events() -> dict[str, list[str]]:
            return {
                document_id: [
                    event.type
                    for event in await service.store.list_events(
                        run_id="run_routing",
                        document_id=document_id,
                    )
                ]
                for document_id in ("case.pdf", "mail.eml")
            }

        events = asyncio.run(read_events())
        self.assertIn("process.skipped", events["case.pdf"])
        self.assertIn("process.skipped", events["mail.eml"])

    def test_process_output_can_spawn_child_documents_for_fan_out(self) -> None:
        parent_pipeline = PipelineSpec(
            id="parent_flow",
            steps=[
                ProcessSpec(
                    id="split",
                    adapter=AdapterSpec(kind="queue", queue="documents.split"),
                )
            ],
            reduces=[
                RunReduceSpec(
                    id="split_page_counts",
                    process_id="split",
                    mode="collect_values",
                    value_key="page_count",
                ),
                RunReduceSpec(
                    id="split_output_count",
                    process_id="split",
                    mode="count",
                ),
            ],
        )
        child_pipeline = PipelineSpec(
            id="page_flow",
            steps=[
                ProcessSpec(
                    id="parse_page",
                    adapter=AdapterSpec(kind="queue", queue="documents.page.parse"),
                )
            ],
        )
        service = RuntimeService(
            registry=PipelineRegistry([parent_pipeline, child_pipeline]),
            store=InMemoryStateStore(),
        )

        async def exercise() -> tuple[
            ProcessOutput,
            list[dict],
            ClaimedProcess,
            dict,
            dict,
            dict,
            dict,
            dict,
        ]:
            await service.initialize_document(
                run_id="run_fanout",
                document_id="case.pdf",
                pipeline_id="parent_flow",
                values={"case_id": "C-1"},
            )
            parent_claim = await service.claim_next(
                run_id="run_fanout",
                pipeline_id="parent_flow",
                worker_id="split-worker",
                adapter_kind="queue",
            )
            self.assertIsNotNone(parent_claim)
            assert parent_claim is not None
            output, _refreshed, _schedule, spawned = await service.complete_process_output(
                run_id="run_fanout",
                document_id="case.pdf",
                process_id="split",
                pipeline_id="parent_flow",
                worker_id="split-worker",
                output=ProcessOutput(
                    values={"page_count": 1},
                    spawn_documents=[
                        SpawnDocumentInput(
                            document_id="case.pdf#page-1",
                            pipeline_id="page_flow",
                            title="case.pdf page 1",
                            document_type="pdf_page",
                            relation="page",
                            media_type="application/pdf",
                            source_uri="file:///tmp/case-page-1.pdf",
                            values={"page_number": 1},
                        )
                    ],
                ),
            )
            child_claim = await service.claim_next(
                run_id="run_fanout",
                pipeline_id="page_flow",
                worker_id="page-worker",
                adapter_kind="queue",
            )
            self.assertIsNotNone(child_claim)
            assert child_claim is not None
            child_document = await service.store.get_document(
                run_id="run_fanout",
                document_id="case.pdf#page-1",
            )
            self.assertIsNotNone(child_document)
            assert child_document is not None
            child_input = await service.store.get_document_input(
                run_id="run_fanout",
                document_id="case.pdf#page-1",
            )
            self.assertIsNotNone(child_input)
            assert child_input is not None
            state = await service.load_state_model("run_fanout")
            lineage = await service.document_lineage("run_fanout")
            results = await service.run_results(
                "run_fanout",
                pipeline_id="parent_flow",
                process_id="split",
            )
            reductions = await service.run_reductions(
                "run_fanout",
                pipeline_id="parent_flow",
            )
            return (
                output,
                [schedule.model_dump(mode="json") for schedule in spawned],
                child_claim,
                {
                    "document": child_document.model_dump(mode="json"),
                    "input": child_input.model_dump(mode="json"),
                },
                state.model_dump(mode="json"),
                lineage.model_dump(mode="json"),
                results.model_dump(mode="json"),
                reductions.model_dump(mode="json"),
            )

        (
            output,
            spawned,
            child_claim,
            child,
            state,
            lineage,
            results,
            reductions,
        ) = asyncio.run(exercise())

        self.assertEqual(output.values, {"page_count": 1})
        self.assertEqual(len(output.spawn_documents), 1)
        self.assertEqual(len(spawned), 1)
        self.assertEqual(spawned[0]["document_id"], "case.pdf#page-1")
        self.assertEqual([item["id"] for item in spawned[0]["queued"]], ["parse_page"])
        self.assertEqual(child_claim.document_id, "case.pdf#page-1")
        self.assertEqual(child_claim.process.id, "parse_page")
        self.assertEqual(child["document"]["pipeline_id"], "page_flow")
        self.assertEqual(child["document"]["parent_document_id"], "case.pdf")
        self.assertEqual(child["document"]["parent_process_id"], "split")
        self.assertEqual(child["document"]["relation"], "page")
        self.assertEqual(
            child["input"]["values"]["document"]["parent_document_id"],
            "case.pdf",
        )
        self.assertEqual(
            child["input"]["values"]["document"]["parent_process_id"],
            "split",
        )
        self.assertEqual(
            child["input"]["values"]["document"]["relation"],
            "page",
        )
        self.assertEqual(state["summary"]["root_document_count"], 1)
        self.assertEqual(state["summary"]["child_document_count"], 1)
        parent_state = next(
            item for item in state["documents"] if item["document_id"] == "case.pdf"
        )
        child_state = next(
            item
            for item in state["documents"]
            if item["document_id"] == "case.pdf#page-1"
        )
        self.assertEqual(parent_state["child_document_ids"], ["case.pdf#page-1"])
        self.assertEqual(child_state["parent_document_id"], "case.pdf")
        self.assertEqual(child_state["relation"], "page")
        self.assertEqual(lineage["root_document_ids"], ["case.pdf"])
        self.assertEqual(lineage["node_count"], 2)
        self.assertEqual(lineage["edge_count"], 1)
        self.assertEqual(
            lineage["edges"][0],
            {
                "parent_document_id": "case.pdf",
                "child_document_id": "case.pdf#page-1",
                "parent_process_id": "split",
                "relation": "page",
                "child_pipeline_id": "page_flow",
            },
        )
        self.assertEqual(results["count"], 1)
        self.assertEqual(results["filters"], {"pipeline_id": "parent_flow", "process_id": "split"})
        self.assertEqual(results["results"][0]["document_id"], "case.pdf")
        self.assertEqual(results["results"][0]["process_id"], "split")
        self.assertEqual(results["results"][0]["value_keys"], ["page_count"])
        self.assertEqual(results["results"][0]["lineage"]["process_id"], "split")
        self.assertEqual(reductions["count"], 2)
        collected = next(
            item for item in reductions["reductions"] if item["id"] == "split_page_counts"
        )
        self.assertEqual(collected["result_count"], 1)
        self.assertEqual(
            collected["output"]["values"]["items"][0]["value"],
            1,
        )
        counted = next(
            item for item in reductions["reductions"] if item["id"] == "split_output_count"
        )
        self.assertEqual(counted["output"]["values"]["count"], 1)
        self.assertEqual(counted["output"]["values"]["by_process"], {"split": 1})

        async def fetch_api_models() -> tuple[dict, dict, dict]:
            app = FastAPI()
            app.include_router(create_runtime_router(service), prefix="/api")
            async with ProcessRuntimeClient(
                "http://runtime.test",
                transport=httpx.ASGITransport(app=app),
            ) as client:
                return (
                    await client.document_lineage(run_id="run_fanout"),
                    await client.run_results(
                        run_id="run_fanout",
                        pipeline_id="parent_flow",
                        process_id="split",
                    ),
                    await client.run_reductions(
                        run_id="run_fanout",
                        pipeline_id="parent_flow",
                        reduce_id="split_page_counts",
                    ),
                )

        api_lineage, api_results, api_reductions = asyncio.run(fetch_api_models())
        self.assertEqual(api_lineage["node_count"], 2)
        self.assertEqual(api_lineage["edge_count"], 1)
        self.assertEqual(api_results["count"], 1)
        self.assertEqual(api_results["results"][0]["document_id"], "case.pdf")
        self.assertEqual(api_reductions["count"], 1)
        self.assertEqual(api_reductions["reductions"][0]["id"], "split_page_counts")

    def test_parent_process_can_wait_for_spawned_children_before_join(self) -> None:
        parent_pipeline = PipelineSpec(
            id="parent_flow",
            steps=[
                ProcessSpec(
                    id="split",
                    adapter=AdapterSpec(kind="queue", queue="documents.split"),
                ),
                ProcessSpec(
                    id="join",
                    adapter=AdapterSpec(kind="queue", queue="documents.join"),
                    needs=["split"],
                    wait_for_children=ChildDocumentWaitSpec(
                        from_processes=["split"],
                        document_types=["pdf_page"],
                        relations=["page"],
                        min_count=1,
                    ),
                ),
            ],
        )
        child_pipeline = PipelineSpec(
            id="page_flow",
            steps=[
                ProcessSpec(
                    id="parse_page",
                    adapter=AdapterSpec(kind="queue", queue="documents.page.parse"),
                )
            ],
        )
        service = RuntimeService(
            registry=PipelineRegistry([parent_pipeline, child_pipeline]),
            store=InMemoryStateStore(),
        )

        async def exercise() -> tuple[dict, dict, ClaimedProcess | None, dict]:
            await service.initialize_document(
                run_id="run_join",
                document_id="case.pdf",
                pipeline_id="parent_flow",
            )
            await service.complete_process_output(
                run_id="run_join",
                document_id="case.pdf",
                pipeline_id="parent_flow",
                process_id="split",
                output=ProcessOutput(
                    values={"page_count": 1},
                    spawn_documents=[
                        SpawnDocumentInput(
                            document_id="case.pdf#page-1",
                            pipeline_id="page_flow",
                            document_type="pdf_page",
                            relation="page",
                            media_type="application/pdf",
                            source_uri="file:///tmp/case-page-1.pdf",
                        )
                    ],
                ),
            )
            blocked = await service.schedule_document(
                run_id="run_join",
                document_id="case.pdf",
                pipeline_id="parent_flow",
            )
            child_claim = await service.claim_next(
                run_id="run_join",
                pipeline_id="page_flow",
                worker_id="page-worker",
                adapter_kind="queue",
            )
            self.assertIsNotNone(child_claim)
            assert child_claim is not None
            await service.complete_process_output(
                run_id="run_join",
                document_id="case.pdf#page-1",
                pipeline_id="page_flow",
                process_id="parse_page",
                worker_id="page-worker",
                output=ProcessOutput(values={"text": "page text"}),
            )
            join_claim = await service.claim_next(
                run_id="run_join",
                pipeline_id="parent_flow",
                worker_id="join-worker",
                adapter_kind="queue",
            )
            events = await service.store.list_events(
                run_id="run_join",
                document_id="case.pdf",
            )
            return (
                blocked.model_dump(mode="json"),
                {
                    event.type: event.model_dump(mode="json")
                    for event in events
                    if event.type == "process.waiting_for_children"
                },
                join_claim,
                (await service.load_state_model("run_join")).model_dump(mode="json"),
            )

        blocked, waiting_events, join_claim, state = asyncio.run(exercise())

        self.assertEqual(blocked["waiting"], ["join"])
        self.assertIn("process.waiting_for_children", waiting_events)
        event = waiting_events["process.waiting_for_children"]
        self.assertEqual(event["data"]["matched_child_count"], 1)
        self.assertEqual(event["data"]["waiting_child_count"], 1)
        self.assertEqual(event["data"]["filters"]["from_processes"], ["split"])
        self.assertEqual(event["data"]["filters"]["document_types"], ["pdf_page"])
        self.assertEqual(event["data"]["filters"]["relations"], ["page"])
        self.assertIsNotNone(join_claim)
        assert join_claim is not None
        self.assertEqual(join_claim.document_id, "case.pdf")
        self.assertEqual(join_claim.process.id, "join")
        parent = next(
            item for item in state["documents"] if item["document_id"] == "case.pdf"
        )
        child = next(
            item
            for item in state["documents"]
            if item["document_id"] == "case.pdf#page-1"
        )
        self.assertEqual(parent["statuses"]["join"], "running")
        self.assertEqual(child["statuses"]["parse_page"], "completed")

    def test_process_output_auto_routes_spawned_child_documents_from_contracts(self) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="document_pkg",
                pipelines=["email.yaml", "page.yaml"],
                document_types=[
                    DocumentTypeSpec(
                        id="email_document",
                        media_types=["message/rfc822"],
                        extensions=[".eml"],
                    ),
                    DocumentTypeSpec(
                        id="pdf_page",
                        media_types=["application/pdf"],
                        extensions=[".pdf"],
                    ),
                ],
                capabilities=[
                    CapabilitySpec(
                        id="split_email",
                        accepts_document_types=["email_document"],
                        emits_document_types=["pdf_page"],
                    ),
                    CapabilitySpec(
                        id="parse_page",
                        accepts_document_types=["pdf_page"],
                    ),
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="email_flow",
                steps=[
                    ProcessSpec(
                        id="split",
                        capability="split_email",
                        adapter=AdapterSpec(kind="queue", queue="email.split"),
                    )
                ],
            ),
            package_id="document_pkg",
        )
        registry.add(
            PipelineSpec(
                id="page_flow",
                steps=[
                    ProcessSpec(
                        id="parse",
                        capability="parse_page",
                        adapter=AdapterSpec(kind="queue", queue="page.parse"),
                    )
                ],
            ),
            package_id="document_pkg",
        )
        service = RuntimeService(registry=registry, store=InMemoryStateStore())

        async def exercise() -> tuple[dict, dict, dict, list[str]]:
            await service.initialize_document(
                run_id="run_spawn_route",
                document_id="mail.eml",
                pipeline_id="email_flow",
                values={"document": {"document_type": "email_document"}},
            )
            output, _refreshed, _schedule, spawned = await service.complete_process_output(
                run_id="run_spawn_route",
                document_id="mail.eml",
                process_id="split",
                pipeline_id="email_flow",
                worker_id="split-worker",
                output=ProcessOutput(
                    values={"attachment_count": 1},
                    spawn_documents=[
                        SpawnDocumentInput(
                            document_id="mail.eml#page-1",
                            media_type="application/pdf",
                            source_uri="file:///tmp/page-1.pdf",
                            values={"page_number": 1},
                        )
                    ],
                ),
            )
            child = await service.store.get_document(
                run_id="run_spawn_route",
                document_id="mail.eml#page-1",
            )
            self.assertIsNotNone(child)
            assert child is not None
            events = await service.store.list_events(
                run_id="run_spawn_route",
                document_id="mail.eml#page-1",
            )
            return (
                output.model_dump(mode="json"),
                child.model_dump(mode="json"),
                spawned[0].model_dump(mode="json"),
                [
                    event.model_dump(mode="json")["data"]["route"]["routes"][0][
                        "route_id"
                    ]
                    for event in events
                    if event.type == "document.spawned"
                ],
            )

        output, child, spawned, event_route_ids = asyncio.run(exercise())

        route_report = output["metadata"]["process_runtime"]["spawn_route_report"]
        self.assertEqual(route_report["document_count"], 1)
        self.assertEqual(route_report["routed_count"], 1)
        self.assertEqual(route_report["fallback_pipeline_count"], 0)
        self.assertEqual(
            route_report["documents"][0]["routes"][0]["route_id"],
            "auto:page_flow:pdf_page",
        )
        self.assertEqual(
            route_report["documents"][0]["final"]["pipeline_id"],
            "page_flow",
        )
        self.assertEqual(
            route_report["documents"][0]["final"]["document_type"],
            "pdf_page",
        )
        self.assertEqual(child["pipeline_id"], "page_flow")
        self.assertEqual(child["document_type"], "pdf_page")
        self.assertEqual(child["parent_document_id"], "mail.eml")
        self.assertEqual(spawned["pipeline_id"], "page_flow")
        self.assertEqual([item["id"] for item in spawned["queued"]], ["parse"])
        self.assertEqual(event_route_ids, ["auto:page_flow:pdf_page"])

    def test_process_output_rejects_spawned_document_type_not_emitted_by_capability(
        self,
    ) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="document_pkg",
                pipelines=["email.yaml", "page.yaml"],
                document_types=[
                    DocumentTypeSpec(id="email_document"),
                    DocumentTypeSpec(id="pdf_page"),
                ],
                capabilities=[
                    CapabilitySpec(
                        id="split_email",
                        accepts_document_types=["email_document"],
                        emits_document_types=["pdf_page"],
                    ),
                    CapabilitySpec(
                        id="parse_page",
                        accepts_document_types=["pdf_page"],
                    ),
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="email_flow",
                steps=[
                    ProcessSpec(
                        id="split",
                        capability="split_email",
                        adapter=AdapterSpec(kind="queue", queue="email.split"),
                    )
                ],
            ),
            package_id="document_pkg",
        )
        registry.add(
            PipelineSpec(
                id="page_flow",
                steps=[
                    ProcessSpec(
                        id="parse",
                        capability="parse_page",
                        adapter=AdapterSpec(kind="queue", queue="page.parse"),
                    )
                ],
            ),
            package_id="document_pkg",
        )
        service = RuntimeService(registry=registry, store=InMemoryStateStore())

        async def exercise() -> None:
            await service.initialize_document(
                run_id="run_spawn_contract",
                document_id="mail.eml",
                pipeline_id="email_flow",
                values={"document": {"document_type": "email_document"}},
            )
            with self.assertRaisesRegex(
                ValueError,
                "type 'email_document'.*is not emitted by capability 'split_email'",
            ):
                await service.complete_process_output(
                    run_id="run_spawn_contract",
                    document_id="mail.eml",
                    process_id="split",
                    pipeline_id="email_flow",
                    output=ProcessOutput(
                        spawn_documents=[
                            SpawnDocumentInput(
                                document_id="mail.eml#nested",
                                document_type="email_document",
                            )
                        ],
                    ),
                )
            self.assertIsNone(
                await service.store.get_document(
                    run_id="run_spawn_contract",
                    document_id="mail.eml#nested",
                )
            )
            self.assertIsNone(
                await service.store.get_output(
                    run_id="run_spawn_contract",
                    document_id="mail.eml",
                    process_id="split",
                )
            )

        asyncio.run(exercise())

    def test_process_output_uses_emitted_document_types_to_route_ambiguous_spawn(
        self,
    ) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="pdf_pkg",
                pipelines=["split.yaml", "page.yaml"],
                document_types=[
                    DocumentTypeSpec(
                        id="source_pdf",
                        media_types=["application/pdf"],
                        extensions=[".pdf"],
                    ),
                    DocumentTypeSpec(
                        id="pdf_page",
                        media_types=["application/pdf"],
                        extensions=[".pdf"],
                    ),
                ],
                document_relations=[
                    DocumentRelationSpec(
                        id="page",
                        source_document_types=["source_pdf"],
                        target_document_types=["pdf_page"],
                    )
                ],
                capabilities=[
                    CapabilitySpec(
                        id="split_pdf",
                        accepts_document_types=["source_pdf"],
                        emits_document_types=["pdf_page"],
                    ),
                    CapabilitySpec(
                        id="parse_page",
                        accepts_document_types=["pdf_page"],
                    ),
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="split_flow",
                steps=[
                    ProcessSpec(
                        id="split",
                        capability="split_pdf",
                        adapter=AdapterSpec(kind="queue", queue="pdf.split"),
                    )
                ],
            ),
            package_id="pdf_pkg",
        )
        registry.add(
            PipelineSpec(
                id="page_flow",
                steps=[
                    ProcessSpec(
                        id="parse",
                        capability="parse_page",
                        adapter=AdapterSpec(kind="queue", queue="page.parse"),
                    )
                ],
            ),
            package_id="pdf_pkg",
        )
        service = RuntimeService(registry=registry, store=InMemoryStateStore())

        async def exercise() -> tuple[dict, dict, dict]:
            await service.initialize_documents(
                run_id="run_spawn_ambiguous",
                documents=[
                    RuntimeDocumentInput(
                        document_id="contract.pdf",
                        pipeline_id="split_flow",
                        document_type="source_pdf",
                        media_type="application/pdf",
                        source_uri="file:///tmp/contract.pdf",
                    )
                ],
            )
            output, _refreshed, _schedule, spawned = await service.complete_process_output(
                run_id="run_spawn_ambiguous",
                document_id="contract.pdf",
                process_id="split",
                pipeline_id="split_flow",
                output=ProcessOutput(
                    spawn_documents=[
                        SpawnDocumentInput(
                            document_id="contract.pdf#page-1",
                            relation="page",
                            media_type="application/pdf",
                            source_uri="file:///tmp/contract-page-1.pdf",
                        )
                    ],
                ),
            )
            child = await service.store.get_document(
                run_id="run_spawn_ambiguous",
                document_id="contract.pdf#page-1",
            )
            self.assertIsNotNone(child)
            assert child is not None
            return (
                output.model_dump(mode="json"),
                spawned[0].model_dump(mode="json"),
                child.model_dump(mode="json"),
            )

        output, spawned, child = asyncio.run(exercise())

        route_report = output["metadata"]["process_runtime"]["spawn_route_report"]
        self.assertEqual(route_report["auto_route_count"], 1)
        self.assertEqual(
            route_report["documents"][0]["routes"][0]["route_id"],
            "auto:page_flow:pdf_page",
        )
        self.assertEqual(
            route_report["documents"][0]["final"]["pipeline_id"],
            "page_flow",
        )
        self.assertEqual(
            route_report["documents"][0]["final"]["document_type"],
            "pdf_page",
        )
        self.assertEqual(spawned["pipeline_id"], "page_flow")
        self.assertEqual([item["id"] for item in spawned["queued"]], ["parse"])
        self.assertEqual(child["pipeline_id"], "page_flow")
        self.assertEqual(child["document_type"], "pdf_page")
        self.assertEqual(child["relation"], "page")

    def test_runtime_cli_complete_process_can_spawn_documents_from_output_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipeline_dir = root / "pipelines"
            pipeline_dir.mkdir()
            (pipeline_dir / "parent.yaml").write_text(
                textwrap.dedent(
                    """
                    id: parent_cli
                    steps:
                      - id: split
                        adapter:
                          kind: manual
                    reduces:
                      - id: split_page_counts
                        process_id: split
                        mode: collect_values
                        value_key: page_count
                    """
                ),
                encoding="utf-8",
            )
            (pipeline_dir / "child.yaml").write_text(
                textwrap.dedent(
                    """
                    id: child_cli
                    steps:
                      - id: parse
                        adapter:
                          kind: queue
                          queue: child.parse
                    """
                ),
                encoding="utf-8",
            )
            db_path = root / "runtime.db"
            output_path = root / "split-output.json"
            output_path.write_text(
                json.dumps(
                    {
                        "values": {"page_count": 1},
                        "spawn_documents": [
                            {
                                "document_id": "doc.pdf#page-1",
                                "pipeline_id": "child_cli",
                                "document_type": "pdf_page",
                                "source_uri": "file:///tmp/page-1.pdf",
                                "values": {"page_number": 1},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "init-document",
                "--db",
                str(db_path),
                "--pipeline",
                "parent_cli",
                "--run-id",
                "run_cli_spawn",
                "--document-id",
                "doc.pdf",
            )
            completed = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "complete-process",
                "--db",
                str(db_path),
                "--pipeline",
                "parent_cli",
                "--run-id",
                "run_cli_spawn",
                "--document-id",
                "doc.pdf",
                "--process-id",
                "split",
                "--output-file",
                str(output_path),
            )

            self.assertEqual(
                completed["spawned_documents"][0]["document_id"],
                "doc.pdf#page-1",
            )
            self.assertEqual(
                [item["id"] for item in completed["spawned_documents"][0]["queued"]],
                ["parse"],
            )
            claim = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "claim",
                "--db",
                str(db_path),
                "--pipeline",
                "child_cli",
                "--run-id",
                "run_cli_spawn",
                "--worker-id",
                "child-worker",
                "--adapter-kind",
                "queue",
            )
            self.assertEqual(claim["claim"]["document_id"], "doc.pdf#page-1")
            self.assertEqual(claim["claim"]["process"]["id"], "parse")
            lineage = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "document-lineage",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli_spawn",
            )
            self.assertEqual(lineage["lineage"]["root_document_ids"], ["doc.pdf"])
            self.assertEqual(lineage["lineage"]["edge_count"], 1)
            self.assertEqual(
                lineage["lineage"]["edges"][0]["child_document_id"],
                "doc.pdf#page-1",
            )
            results = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "run-results",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli_spawn",
                "--pipeline",
                "parent_cli",
                "--process-id",
                "split",
            )
            self.assertEqual(results["results"]["count"], 1)
            self.assertEqual(
                results["results"]["results"][0]["value_keys"],
                ["page_count"],
            )
            jsonl_buffer = StringIO()
            with redirect_stdout(jsonl_buffer):
                code = runtime_cli_main(
                    [
                        "--pipeline-dir",
                        str(pipeline_dir),
                        "run-results",
                        "--db",
                        str(db_path),
                        "--run-id",
                        "run_cli_spawn",
                        "--pipeline",
                        "parent_cli",
                        "--process-id",
                        "split",
                        "--jsonl",
                    ]
                )
            self.assertEqual(code, 0, jsonl_buffer.getvalue())
            jsonl_lines = [
                json.loads(line)
                for line in jsonl_buffer.getvalue().splitlines()
                if line.strip()
            ]
            self.assertEqual(len(jsonl_lines), 1)
            self.assertEqual(jsonl_lines[0]["document_id"], "doc.pdf")
            reductions = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "run-reductions",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli_spawn",
                "--pipeline",
                "parent_cli",
                "--reduce-id",
                "split_page_counts",
            )
            self.assertEqual(reductions["reductions"]["count"], 1)
            self.assertEqual(
                reductions["reductions"]["reductions"][0]["output"]["values"]["items"][0]["value"],
                1,
            )

    def test_runtime_cli_complete_process_finishes_manual_gate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipeline_dir = root / "pipelines"
            pipeline_dir.mkdir()
            (pipeline_dir / "manual.yaml").write_text(
                textwrap.dedent(
                    """
                    id: manual_cli
                    steps:
                      - id: review
                        adapter:
                          kind: manual
                      - id: publish
                        needs: [review]
                        adapter:
                          kind: queue
                          queue: manual.publish
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
                "manual_cli",
                "--run-id",
                "run_manual_cli",
                "--document-id",
                "doc.txt",
            )
            claim = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "claim",
                "--db",
                str(db_path),
                "--pipeline",
                "manual_cli",
                "--run-id",
                "run_manual_cli",
                "--worker-id",
                "worker-cli",
                "--adapter-kind",
                "queue",
            )
            self.assertIsNone(claim["claim"])

            completed = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "complete-process",
                "--db",
                str(db_path),
                "--pipeline",
                "manual_cli",
                "--run-id",
                "run_manual_cli",
                "--document-id",
                "doc.txt",
                "--process-id",
                "review",
                "--value",
                "approved=yes",
            )
            self.assertTrue(completed["ok"])
            self.assertEqual(completed["output"]["values"], {"approved": "yes"})
            self.assertEqual(
                [item["id"] for item in completed["schedule"]["queued"]],
                ["publish"],
            )

            status = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "status",
                "--db",
                str(db_path),
                "--run-id",
                "run_manual_cli",
                "--include-events",
            )
            document = status["state"]["documents"][0]
            self.assertEqual(document["statuses"]["review"], "completed")
            self.assertEqual(document["statuses"]["publish"], "queued")
            self.assertIn(
                "process.manual_required",
                [event["type"] for event in document["events"]],
            )

    def test_runtime_api_rejects_output_artifact_kind_not_emitted_by_capability(self) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="pkg",
                pipelines=["typed.yaml"],
                document_types=[DocumentTypeSpec(id="generic_document")],
                artifact_kinds=[
                    ArtifactKindSpec(id="text"),
                    ArtifactKindSpec(id="image"),
                ],
                capabilities=[
                    CapabilitySpec(
                        id="extract_text",
                        accepts_document_types=["generic_document"],
                        emits_artifact_kinds=["text"],
                    )
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="typed_flow",
                steps=[
                    ProcessSpec(
                        id="extract",
                        capability="extract_text",
                        adapter=AdapterSpec(kind="queue", queue="typed.extract"),
                    )
                ],
            ),
            package_id="pkg",
        )
        registry.validate_package_workers("pkg")
        store = InMemoryStateStore()
        service = RuntimeService(registry=registry, store=store)
        app = FastAPI()
        app.include_router(create_runtime_router(service), prefix="/api")

        async def exercise() -> None:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_bad_output_kind",
                    pipeline_id="typed_flow",
                    documents=[
                        RuntimeDocumentInput(
                            document_id="doc.txt",
                            document_type="generic_document",
                        )
                    ],
                )
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://runtime.test",
            ) as client:
                write = await client.put(
                    "/api/runs/run_bad_output_kind/process-runtime/"
                    "doc.txt/processes/extract/output",
                    json=ProcessOutput(
                        artifacts=[
                            ArtifactRef(
                                id="preview",
                                kind="image",
                                uri="s3://bucket/preview.png",
                            )
                        ],
                    ).model_dump(mode="json"),
                )
                self.assertEqual(write.status_code, 400, write.text)
                self.assertIn("not emitted by capability", write.json()["detail"])
            stored = await store.get_output(
                run_id="run_bad_output_kind",
                document_id="doc.txt",
                process_id="extract",
            )
            self.assertIsNone(stored)

        asyncio.run(exercise())

    def test_process_output_can_declare_typed_output_documents(self) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="pkg",
                pipelines=["typed.yaml"],
                document_types=[
                    DocumentTypeSpec(id="generic_document"),
                    DocumentTypeSpec(
                        id="redacted_document",
                        media_types=["application/pdf"],
                        extensions=[".pdf"],
                        value_schema={
                            "type": "object",
                            "required": ["source"],
                            "properties": {"source": {"type": "string"}},
                            "additionalProperties": True,
                        },
                        metadata_schema={
                            "type": "object",
                            "required": ["filename"],
                            "properties": {"filename": {"type": "string"}},
                            "additionalProperties": True,
                        },
                    ),
                ],
                document_relations=[
                    DocumentRelationSpec(
                        id="redacted",
                        source_document_types=["generic_document"],
                        target_document_types=["redacted_document"],
                    )
                ],
                artifact_kinds=[
                    ArtifactKindSpec(
                        id="redacted_file",
                        media_types=["application/pdf"],
                        extensions=[".pdf"],
                    )
                ],
                capabilities=[
                    CapabilitySpec(
                        id="redact_document",
                        accepts_document_types=["generic_document"],
                        emits_document_types=["redacted_document"],
                        emits_artifact_kinds=["redacted_file"],
                    )
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="typed_flow",
                steps=[
                    ProcessSpec(
                        id="redact",
                        capability="redact_document",
                        adapter=AdapterSpec(kind="queue", queue="typed.redact"),
                    )
                ],
            ),
            package_id="pkg",
        )
        registry.validate_package_workers("pkg")
        service = RuntimeService(registry=registry, store=InMemoryStateStore())

        async def exercise() -> tuple[dict, dict, dict, dict, list[dict], dict]:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_output_document",
                    pipeline_id="typed_flow",
                    documents=[
                        RuntimeDocumentInput(
                            document_id="source.pdf",
                            document_type="generic_document",
                        )
                    ],
                )
            )
            await service.complete_process_output(
                run_id="run_output_document",
                document_id="source.pdf",
                pipeline_id="typed_flow",
                process_id="redact",
                output=ProcessOutput(
                    values={"status": "ok"},
                    artifacts=[
                        ArtifactRef(
                            id="redacted_pdf",
                            kind="redacted_file",
                            uri="s3://bucket/redacted.pdf",
                            metadata={
                                "media_type": "application/pdf",
                                "filename": "redacted.pdf",
                            },
                        )
                    ],
                    output_documents=[
                        OutputDocumentRef(
                            id="redacted_doc",
                            document_type="redacted_document",
                            media_type="application/pdf",
                            uri="s3://bucket/redacted.pdf",
                            artifact_id="redacted_pdf",
                            relation="redacted",
                            values={"source": "source.pdf"},
                            metadata={"filename": "redacted.pdf"},
                        )
                    ],
                ),
            )
            state = await service.load_state_model("run_output_document")
            results = await service.run_results(
                "run_output_document",
                pipeline_id="typed_flow",
                process_id="redact",
            )
            output_documents = await service.output_documents(
                "run_output_document",
                pipeline_id="typed_flow",
                process_id="redact",
                document_id="source.pdf",
                source_document_type="generic_document",
                document_type="redacted_document",
                relation="redacted",
                media_type="application/pdf",
            )
            missing_documents = await service.output_documents(
                "run_output_document",
                document_type="translated_document",
            )
            app = FastAPI()
            app.include_router(create_runtime_router(service), prefix="/api")
            async with ProcessRuntimeClient(
                "http://runtime.test",
                transport=httpx.ASGITransport(app=app),
            ) as client:
                api_page = await client.output_document_page(
                    run_id="run_output_document",
                    document_type="redacted_document",
                    relation="redacted",
                )
                api_items = await client.output_documents(
                    run_id="run_output_document",
                    output_document_id="redacted_doc",
                )
            return (
                results.model_dump(mode="json"),
                output_documents.model_dump(mode="json"),
                missing_documents.model_dump(mode="json"),
                api_page.model_dump(mode="json"),
                api_items,
                state.model_dump(mode="json"),
            )

        (
            results,
            output_documents,
            missing_documents,
            api_page,
            api_items,
            state,
        ) = asyncio.run(exercise())

        self.assertEqual(results["count"], 1)
        result = results["results"][0]
        self.assertEqual(result["output_document_count"], 1)
        self.assertEqual(
            result["output"]["output_documents"][0]["document_type"],
            "redacted_document",
        )
        self.assertEqual(result["output"]["output_documents"][0]["relation"], "redacted")
        self.assertEqual(result["lineage"]["output_document_count"], 1)
        self.assertEqual(
            result["lineage"]["output_documents"][0]["document_type"],
            "redacted_document",
        )
        self.assertEqual(result["artifact_count"], 1)
        self.assertEqual(state["summary"]["output_document_count"], 1)
        self.assertEqual(
            state["documents"][0]["steps"][0]["output_document_count"],
            1,
        )
        self.assertEqual(output_documents["count"], 1)
        self.assertFalse(output_documents["has_more"])
        self.assertEqual(
            output_documents["filters"],
            {
                "pipeline_id": "typed_flow",
                "process_id": "redact",
                "document_id": "source.pdf",
                "source_document_type": "generic_document",
                "document_type": "redacted_document",
                "relation": "redacted",
                "media_type": "application/pdf",
            },
        )
        output_item = output_documents["output_documents"][0]
        self.assertEqual(output_item["source_document_id"], "source.pdf")
        self.assertEqual(output_item["source_document_type"], "generic_document")
        self.assertEqual(output_item["output_document_id"], "redacted_doc")
        self.assertEqual(output_item["document_type"], "redacted_document")
        self.assertEqual(output_item["relation"], "redacted")
        self.assertEqual(output_item["artifact"]["kind"], "redacted_file")
        self.assertEqual(output_item["value_keys"], ["source"])
        self.assertEqual(output_item["metadata_keys"], ["filename"])
        self.assertEqual(missing_documents["count"], 0)
        self.assertEqual(api_page["count"], 1)
        self.assertEqual(api_page["output_documents"][0]["output_document_id"], "redacted_doc")
        self.assertEqual(len(api_items), 1)
        self.assertEqual(api_items[0]["artifact_id"], "redacted_pdf")

    def test_process_output_rejects_output_document_type_not_emitted_by_capability(
        self,
    ) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="pkg",
                pipelines=["typed.yaml"],
                document_types=[
                    DocumentTypeSpec(id="generic_document"),
                    DocumentTypeSpec(id="redacted_document"),
                ],
                capabilities=[
                    CapabilitySpec(
                        id="redact_document",
                        accepts_document_types=["generic_document"],
                    )
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="typed_flow",
                steps=[
                    ProcessSpec(
                        id="redact",
                        capability="redact_document",
                        adapter=AdapterSpec(kind="queue", queue="typed.redact"),
                    )
                ],
            ),
            package_id="pkg",
        )
        registry.validate_package_workers("pkg")
        service = RuntimeService(registry=registry, store=InMemoryStateStore())

        async def exercise() -> str:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_bad_output_document",
                    pipeline_id="typed_flow",
                    documents=[
                        RuntimeDocumentInput(
                            document_id="source.pdf",
                            document_type="generic_document",
                        )
                    ],
                )
            )
            with self.assertRaisesRegex(
                ValueError,
                "requires capability to declare emitted document types",
            ) as error:
                await service.complete_process_output(
                    run_id="run_bad_output_document",
                    document_id="source.pdf",
                    pipeline_id="typed_flow",
                    process_id="redact",
                    output=ProcessOutput(
                        output_documents=[
                            OutputDocumentRef(
                                id="redacted_doc",
                                document_type="redacted_document",
                            )
                        ],
                    ),
                )
            return str(error.exception)

        message = asyncio.run(exercise())
        self.assertIn("output document 'redacted_doc'", message)

    def test_process_output_rejects_undeclared_output_document_relation(self) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="pkg",
                pipelines=["typed.yaml"],
                document_types=[
                    DocumentTypeSpec(id="generic_document"),
                    DocumentTypeSpec(id="redacted_document"),
                ],
                document_relations=[
                    DocumentRelationSpec(
                        id="redacted",
                        source_document_types=["generic_document"],
                        target_document_types=["redacted_document"],
                    )
                ],
                capabilities=[
                    CapabilitySpec(
                        id="redact_document",
                        accepts_document_types=["generic_document"],
                        emits_document_types=["redacted_document"],
                    )
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="typed_flow",
                steps=[
                    ProcessSpec(
                        id="redact",
                        capability="redact_document",
                        adapter=AdapterSpec(kind="queue", queue="typed.redact"),
                    )
                ],
            ),
            package_id="pkg",
        )
        service = RuntimeService(registry=registry, store=InMemoryStateStore())

        async def exercise() -> str:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_bad_output_relation",
                    pipeline_id="typed_flow",
                    documents=[
                        RuntimeDocumentInput(
                            document_id="source.pdf",
                            document_type="generic_document",
                        )
                    ],
                )
            )
            with self.assertRaisesRegex(ValueError, "relation 'translated'"):
                await service.complete_process_output(
                    run_id="run_bad_output_relation",
                    document_id="source.pdf",
                    pipeline_id="typed_flow",
                    process_id="redact",
                    output=ProcessOutput(
                        output_documents=[
                            OutputDocumentRef(
                                id="translated_doc",
                                document_type="redacted_document",
                                relation="translated",
                            )
                        ],
                    ),
                )
            stored = await service.store.get_output(
                run_id="run_bad_output_relation",
                document_id="source.pdf",
                process_id="redact",
            )
            self.assertIsNone(stored)
            return "ok"

        self.assertEqual(asyncio.run(exercise()), "ok")

    def test_runtime_api_rejects_output_artifact_metadata_not_matching_kind_schema(self) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="pkg",
                pipelines=["typed.yaml"],
                document_types=[DocumentTypeSpec(id="generic_document")],
                artifact_kinds=[
                    ArtifactKindSpec(
                        id="text",
                        metadata_schema={
                            "type": "object",
                            "required": ["page_count"],
                            "properties": {"page_count": {"type": "integer"}},
                            "additionalProperties": True,
                        },
                    )
                ],
                capabilities=[
                    CapabilitySpec(
                        id="extract_text",
                        accepts_document_types=["generic_document"],
                        emits_artifact_kinds=["text"],
                    )
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="typed_flow",
                steps=[
                    ProcessSpec(
                        id="extract",
                        capability="extract_text",
                        adapter=AdapterSpec(kind="queue", queue="typed.extract"),
                    )
                ],
            ),
            package_id="pkg",
        )
        registry.validate_package_workers("pkg")
        store = InMemoryStateStore()
        service = RuntimeService(registry=registry, store=store)
        app = FastAPI()
        app.include_router(create_runtime_router(service), prefix="/api")

        async def exercise() -> None:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_bad_artifact_metadata",
                    pipeline_id="typed_flow",
                    documents=[
                        RuntimeDocumentInput(
                            document_id="doc.txt",
                            document_type="generic_document",
                        )
                    ],
                )
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://runtime.test",
            ) as client:
                write = await client.put(
                    "/api/runs/run_bad_artifact_metadata/process-runtime/"
                    "doc.txt/processes/extract/output",
                    json=ProcessOutput(
                        artifacts=[
                            ArtifactRef(
                                id="payload",
                                kind="text",
                                uri="s3://bucket/payload.txt",
                                metadata={"media_type": "text/plain"},
                            )
                        ],
                    ).model_dump(mode="json"),
                )
                self.assertEqual(write.status_code, 400, write.text)
                self.assertIn("artifact 'payload' metadata", write.json()["detail"])
                self.assertIn("'page_count' is a required property", write.json()["detail"])
            stored = await store.get_output(
                run_id="run_bad_artifact_metadata",
                document_id="doc.txt",
                process_id="extract",
            )
            self.assertIsNone(stored)

        asyncio.run(exercise())

    def test_runtime_api_validates_output_artifact_value_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            valid_path = root / "valid.json"
            valid_path.write_text(json.dumps({"text": "hello"}), encoding="utf-8")
            invalid_path = root / "invalid.json"
            invalid_path.write_text(json.dumps({"text": 42}), encoding="utf-8")
            registry = PipelineRegistry()
            registry.add_package(
                WorkflowPackageSpec(
                    id="pkg",
                    pipelines=["typed.yaml"],
                    document_types=[DocumentTypeSpec(id="generic_document")],
                    artifact_kinds=[
                        ArtifactKindSpec(
                            id="text",
                            media_types=["application/json"],
                            value_schema={
                                "type": "object",
                                "required": ["text"],
                                "properties": {"text": {"type": "string"}},
                                "additionalProperties": True,
                            },
                        )
                    ],
                    capabilities=[
                        CapabilitySpec(
                            id="extract_text",
                            accepts_document_types=["generic_document"],
                            emits_artifact_kinds=["text"],
                        )
                    ],
                )
            )
            registry.add(
                PipelineSpec(
                    id="typed_flow",
                    steps=[
                        ProcessSpec(
                            id="extract",
                            capability="extract_text",
                            adapter=AdapterSpec(kind="queue", queue="typed.extract"),
                        )
                    ],
                ),
                package_id="pkg",
            )
            registry.validate_package_workers("pkg")
            store = InMemoryStateStore()
            service = RuntimeService(
                registry=registry,
                store=store,
                artifact_roots=[root],
                artifact_store_root=root / "artifact-store",
            )
            app = FastAPI()
            app.include_router(create_runtime_router(service), prefix="/api")

            async def exercise() -> None:
                await service.create_run_with_documents(
                    RuntimeRunInput(
                        run_id="run_artifact_value",
                        pipeline_id="typed_flow",
                        documents=[
                            RuntimeDocumentInput(
                                document_id="doc.txt",
                                document_type="generic_document",
                            )
                        ],
                    )
                )
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://runtime.test",
                ) as client:
                    valid_write = await client.put(
                        "/api/runs/run_artifact_value/process-runtime/"
                        "doc.txt/processes/extract/output",
                        json=ProcessOutput(
                            artifacts=[
                                ArtifactRef(
                                    id="payload",
                                    kind="text",
                                    uri=valid_path.as_uri(),
                                    metadata={"media_type": "application/json"},
                                )
                            ],
                        ).model_dump(mode="json"),
                    )
                    self.assertEqual(valid_write.status_code, 200, valid_write.text)
                    stored = await store.get_output(
                        run_id="run_artifact_value",
                        document_id="doc.txt",
                        process_id="extract",
                    )
                    self.assertIsNotNone(stored)
                    assert stored is not None
                    self.assertTrue(stored.artifacts[0].uri.startswith("fala-artifact://"))

                    await service.create_run_with_documents(
                        RuntimeRunInput(
                            run_id="run_bad_artifact_value",
                            pipeline_id="typed_flow",
                            documents=[
                                RuntimeDocumentInput(
                                    document_id="doc.txt",
                                    document_type="generic_document",
                                )
                            ],
                        )
                    )
                    invalid_write = await client.put(
                        "/api/runs/run_bad_artifact_value/process-runtime/"
                        "doc.txt/processes/extract/output",
                        json=ProcessOutput(
                            artifacts=[
                                ArtifactRef(
                                    id="payload",
                                    kind="text",
                                    uri=invalid_path.as_uri(),
                                    metadata={"media_type": "application/json"},
                                )
                            ],
                        ).model_dump(mode="json"),
                    )
                    self.assertEqual(invalid_write.status_code, 400, invalid_write.text)
                    self.assertIn("artifact 'payload' value", invalid_write.json()["detail"])
                    self.assertIn("42 is not of type 'string'", invalid_write.json()["detail"])
                stored_bad = await store.get_output(
                    run_id="run_bad_artifact_value",
                    document_id="doc.txt",
                    process_id="extract",
                )
                self.assertIsNone(stored_bad)

            asyncio.run(exercise())

    def test_runtime_api_rejects_output_artifact_format_not_matching_kind(self) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="pkg",
                pipelines=["typed.yaml"],
                document_types=[DocumentTypeSpec(id="generic_document")],
                artifact_kinds=[
                    ArtifactKindSpec(
                        id="text",
                        media_types=["text/plain"],
                        extensions=[".txt"],
                    )
                ],
                capabilities=[
                    CapabilitySpec(
                        id="extract_text",
                        accepts_document_types=["generic_document"],
                        emits_artifact_kinds=["text"],
                    )
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="typed_flow",
                steps=[
                    ProcessSpec(
                        id="extract",
                        capability="extract_text",
                        adapter=AdapterSpec(kind="queue", queue="typed.extract"),
                    )
                ],
            ),
            package_id="pkg",
        )
        registry.validate_package_workers("pkg")
        store = InMemoryStateStore()
        service = RuntimeService(registry=registry, store=store)
        app = FastAPI()
        app.include_router(create_runtime_router(service), prefix="/api")

        async def exercise() -> None:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_bad_artifact_format",
                    pipeline_id="typed_flow",
                    documents=[
                        RuntimeDocumentInput(
                            document_id="doc.txt",
                            document_type="generic_document",
                        )
                    ],
                )
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://runtime.test",
            ) as client:
                write = await client.put(
                    "/api/runs/run_bad_artifact_format/process-runtime/"
                    "doc.txt/processes/extract/output",
                    json=ProcessOutput(
                        artifacts=[
                            ArtifactRef(
                                id="payload",
                                kind="text",
                                uri="s3://bucket/payload.pdf",
                                metadata={
                                    "media_type": "application/pdf",
                                    "filename": "payload.pdf",
                                },
                            )
                        ],
                    ).model_dump(mode="json"),
                )
                self.assertEqual(write.status_code, 400, write.text)
                self.assertIn("media type 'application/pdf'", write.json()["detail"])
            stored = await store.get_output(
                run_id="run_bad_artifact_format",
                document_id="doc.txt",
                process_id="extract",
            )
            self.assertIsNone(stored)
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_bad_artifact_extension",
                    pipeline_id="typed_flow",
                    documents=[
                        RuntimeDocumentInput(
                            document_id="doc.txt",
                            document_type="generic_document",
                        )
                    ],
                )
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://runtime.test",
            ) as client:
                write = await client.put(
                    "/api/runs/run_bad_artifact_extension/process-runtime/"
                    "doc.txt/processes/extract/output",
                    json=ProcessOutput(
                        artifacts=[
                            ArtifactRef(
                                id="payload",
                                kind="text",
                                uri="s3://bucket/payload.pdf",
                                metadata={
                                    "media_type": "text/plain",
                                    "filename": "payload.pdf",
                                },
                            )
                        ],
                    ).model_dump(mode="json"),
                )
                self.assertEqual(write.status_code, 400, write.text)
                self.assertIn("extension '.pdf'", write.json()["detail"])
            stored = await store.get_output(
                run_id="run_bad_artifact_extension",
                document_id="doc.txt",
                process_id="extract",
            )
            self.assertIsNone(stored)

        asyncio.run(exercise())

    def test_runtime_api_rejects_output_values_not_matching_capability_schema(self) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="pkg",
                pipelines=["typed.yaml"],
                document_types=[DocumentTypeSpec(id="generic_document")],
                capabilities=[
                    CapabilitySpec(
                        id="extract_text",
                        accepts_document_types=["generic_document"],
                        output_schema={
                            "type": "object",
                            "required": ["text"],
                            "properties": {"text": {"type": "string"}},
                            "additionalProperties": False,
                        },
                    )
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="typed_flow",
                steps=[
                    ProcessSpec(
                        id="extract",
                        capability="extract_text",
                        adapter=AdapterSpec(kind="queue", queue="typed.extract"),
                    )
                ],
            ),
            package_id="pkg",
        )
        registry.validate_package_workers("pkg")
        store = InMemoryStateStore()
        service = RuntimeService(registry=registry, store=store)
        app = FastAPI()
        app.include_router(create_runtime_router(service), prefix="/api")

        async def exercise() -> None:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_bad_output_values",
                    pipeline_id="typed_flow",
                    documents=[
                        RuntimeDocumentInput(
                            document_id="doc.txt",
                            document_type="generic_document",
                        )
                    ],
                )
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://runtime.test",
            ) as client:
                write = await client.put(
                    "/api/runs/run_bad_output_values/process-runtime/"
                    "doc.txt/processes/extract/output",
                    json=ProcessOutput(
                        values={"chars": 10},
                    ).model_dump(mode="json"),
                )
                self.assertEqual(write.status_code, 400, write.text)
                self.assertIn("output values", write.json()["detail"])
                self.assertIn("'text' is a required property", write.json()["detail"])
            stored = await store.get_output(
                run_id="run_bad_output_values",
                document_id="doc.txt",
                process_id="extract",
            )
            self.assertIsNone(stored)

        asyncio.run(exercise())

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
                    resources=ResourceSpec(memory_mb=512, labels=["fast"]),
                    lease_seconds=60,
                    renew_interval_seconds=0.01,
                ).run_once(run_id="run_worker")
            )

            self.assertTrue(result.completed)
            self.assertEqual(client.claim_args["resources"].memory_mb, 512)
            self.assertEqual(client.claim_args["resources"].labels, ["fast"])
            self.assertGreaterEqual(len(client.renews), 1)
            self.assertEqual(client.renews[0]["process_id"], "slow")
            self.assertEqual(client.renews[0]["worker_id"], "worker-sdk")
            self.assertEqual(client.outputs[0]["output"].values["ok"], True)
            self.assertEqual(client.worker_heartbeats[0]["resources"].memory_mb, 512)
            self.assertEqual(client.worker_heartbeats[0]["status"].value, "idle")
            self.assertIn(
                "working",
                [heartbeat["status"].value for heartbeat in client.worker_heartbeats],
            )
            self.assertEqual(client.worker_heartbeats[-1]["status"].value, "idle")

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
                    capability="extract_text",
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
                    capability="extract_text",
                    attempt=1,
                    input=ProcessInput(values={"initial": {"source": "doc.pdf"}, "needs": {}}),
                ),
            )
            requests: list[httpx.Request] = []

            async def handler(request: httpx.Request) -> httpx.Response:
                requests.append(request)
                if request.url.path.endswith("/heartbeat"):
                    return httpx.Response(
                        200,
                        json={"ok": True, "worker": json.loads(request.content)},
                    )
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
                        capabilities=["extract_text"],
                    ).run_once(run_id="run_worker")

            result = asyncio.run(run_worker())

            self.assertTrue(result.completed)
            self.assertIsNone(result.error)
            heartbeat_requests = [
                request for request in requests if request.url.path.endswith("/heartbeat")
            ]
            self.assertEqual(
                [json.loads(request.content)["status"] for request in heartbeat_requests],
                ["idle", "working", "idle"],
            )
            claim_request = next(request for request in requests if request.url.path.endswith("/claim"))
            self.assertEqual(claim_request.url.path, "/api/runs/run_worker/process-runtime/claim")
            claim_payload = json.loads(claim_request.content)
            self.assertEqual(claim_payload["capabilities"], ["extract_text"])
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
                if request.url.path.endswith("/heartbeat"):
                    return httpx.Response(
                        200,
                        json={"ok": True, "worker": json.loads(request.content)},
                    )
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
            heartbeat_requests = [
                request for request in requests if request.url.path.endswith("/heartbeat")
            ]
            self.assertEqual(
                [json.loads(request.content)["status"] for request in heartbeat_requests],
                ["idle", "working", "idle"],
            )
            claim_request = next(request for request in requests if request.url.path.endswith("/claim"))
            claim_payload = json.loads(claim_request.content)
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

    def test_adapter_process_runtime_worker_reports_error_kind_on_failure(self) -> None:
        class FailingAdapter:
            async def run(self, *_args, **_kwargs) -> ProcessOutput:
                raise RuntimeError("bad document")

        claim = ClaimedProcess(
            pipeline_id="pipeline",
            run_id="run_worker_error",
            document_id="doc_worker_error",
            process=ScheduledProcess(
                id="extract",
                needs=[],
                adapter={"kind": "queue", "queue": "demo.extract"},
            ),
            worker_id="queue-command-worker",
            attempt=1,
            claim_expires_at=datetime.now(timezone.utc) + timedelta(seconds=60),
            context=ProcessExecutionContext(
                pipeline_id="pipeline",
                run_id="run_worker_error",
                document_id="doc_worker_error",
                process_id="extract",
                attempt=1,
                input=ProcessInput(),
            ),
        )
        client = _FakeRuntimeClient(claim)

        result = asyncio.run(
            AdapterProcessRuntimeWorker(
                client=client,  # type: ignore[arg-type]
                pipeline_id="pipeline",
                worker_id="queue-command-worker",
                adapter_kind="queue",
                adapters=AdapterRegistry({"queue": FailingAdapter()}),  # type: ignore[arg-type]
                error_kind="validation_error",
            ).run_once(run_id="run_worker_error")
        )

        self.assertFalse(result.completed)
        self.assertEqual(result.error, "bad document")
        self.assertEqual(result.error_kind, "validation_error")
        self.assertEqual(client.statuses[0]["status"], ProcessStatus.failed)
        self.assertEqual(client.statuses[0]["data"]["error_kind"], "validation_error")
        self.assertEqual(
            client.worker_heartbeats[-1]["metadata"]["error_kind"],
            "validation_error",
        )

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
                            "capability": ctx.get("capability"),
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
                    capabilities:
                      - id: enrich_document
                    pipelines:
                      - demo.yaml
                    workers:
                      - id: enrich_worker
                        capabilities: [enrich_document]
                        pipeline: demo_flow
                        process: enrich
                        command: ["python", "steps/enrich.py"]
                        cwd: "."
                        resources:
                          memory_mb: 1024
                          labels: [cpu]
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
                        capability: enrich_document
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
                    capability="enrich_document",
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
                    capability="enrich_document",
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
            self.assertEqual(payload["capabilities"], ["enrich_document"])
            self.assertEqual(payload["resources"]["memory_mb"], 1024)
            self.assertEqual(payload["resources"]["labels"], ["cpu"])
            self.assertEqual(payload["completed_count"], 1)
            self.assertEqual(payload["steps"][0]["capability"], "enrich_document")

    def test_supervisor_builds_package_worker_specs_and_cli_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "demo_package"
            package_dir.mkdir()
            (package_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: demo_package
                    capabilities:
                      - id: extract_text
                    secrets:
                      - id: openai_api_key
                        env_var: OPENAI_API_KEY
                        kubernetes_secret_name: fala-openai
                        kubernetes_secret_key: api-key
                    pipelines:
                      - demo.yaml
                    workers:
                      - id: extract_worker
                        capabilities: [extract_text]
                        pipeline: demo_flow
                        process: extract
                        command: ["python", "steps/extract.py"]
                        cwd: "."
                        secrets: [openai_api_key]
                        sandbox:
                          run_as_non_root: true
                          read_only_root_filesystem: true
                        resources:
                          memory_mb: 512
                          gpu_count: 1
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "demo.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: demo_flow
                    steps:
                      - id: extract
                        capability: extract_text
                        adapter:
                          kind: queue
                          queue: demo.extract
                    """
                ).strip(),
                encoding="utf-8",
            )
            registry = PipelineRegistry.from_directory(root)

            specs = build_package_worker_specs(
                registry=registry,
                pipeline_dir=root,
                base_url="http://runtime.test",
                run_id="run_supervisor",
                package_id="demo_package",
            )
            self.assertEqual(len(specs), 1)
            self.assertEqual(specs[0].id, "extract_worker")
            self.assertEqual(specs[0].capabilities, ["extract_text"])
            self.assertIn("--package-worker", specs[0].argv)
            self.assertIn("extract_worker", specs[0].argv)
            self.assertIn("--forever", specs[0].argv)
            self.assertEqual(specs[0].resources.memory_mb, 512)
            self.assertEqual(specs[0].resources.gpu_count, 1)
            self.assertEqual(specs[0].secrets[0].env_var, "OPENAI_API_KEY")
            self.assertTrue(specs[0].sandbox.read_only_root_filesystem)

            payload = _run_cli(
                "--pipeline-dir",
                str(root),
                "supervise-workers",
                "--base-url",
                "http://runtime.test",
                "--run-id",
                "run_supervisor",
                "--package-id",
                "demo_package",
                "--dry-run",
            )

            self.assertTrue(payload["ok"])
            self.assertEqual(payload["worker_count"], 1)
            self.assertEqual(payload["workers"][0]["id"], "extract_worker")
            self.assertEqual(payload["workers"][0]["capabilities"], ["extract_text"])
            self.assertEqual(payload["workers"][0]["resources"]["memory_mb"], 512)
            self.assertEqual(payload["workers"][0]["secrets"][0]["env_var"], "OPENAI_API_KEY")

    def test_serve_starts_bundled_web_control_plane(self) -> None:
        calls: list[dict] = []

        def fake_run(app, **kwargs):
            calls.append({"app": app, **kwargs})

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            buffer = StringIO()
            with patch("uvicorn.run", fake_run), redirect_stdout(buffer):
                code = runtime_cli_main(
                    [
                        "--pipeline-dir",
                        str(SRC_DIR.parent / "examples" / "pipelines"),
                        "serve",
                        "--db",
                        str(root / "fala.db"),
                        "--queue-broker",
                        f"memory://serve-{uuid.uuid4().hex}",
                        "--artifact-root",
                        str(root),
                        "--artifact-store-root",
                        str(root / "artifact-store"),
                        "--title",
                        "Fala Ops",
                        "--host",
                        "0.0.0.0",
                        "--port",
                        "8099",
                        "--log-level",
                        "debug",
                    ]
                )

            self.assertEqual(code, 0)
            self.assertEqual(buffer.getvalue(), "")
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]["app"].title, "Fala Ops")
            self.assertEqual(calls[0]["host"], "0.0.0.0")
            self.assertEqual(calls[0]["port"], 8099)
            self.assertEqual(calls[0]["log_level"], "debug")

            async def fetch_queue() -> None:
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=calls[0]["app"]),
                    base_url="http://testserver",
                ) as client:
                    response = await client.get("/queue")
                    self.assertEqual(response.status_code, 200)
                    self.assertIn("Broker queue", response.text)
                    self.assertIn("No broker work for current filters.", response.text)
                    self.assertNotIn("not configured", response.text)

            asyncio.run(fetch_queue())

    def test_deployment_renders_compose_control_plane_postgres_and_workers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "demo_package"
            package_dir.mkdir()
            (package_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: demo_package
                    capabilities:
                      - id: extract_text
                    secrets:
                      - id: openai_api_key
                        env_var: OPENAI_API_KEY
                    pipelines:
                      - demo.yaml
                    workers:
                      - id: extract_worker
                        capabilities: [extract_text]
                        pipeline: demo_flow
                        process: extract
                        command: ["python", "steps/extract.py"]
                        cwd: "."
                        secrets: [openai_api_key]
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "demo.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: demo_flow
                    steps:
                      - id: extract
                        capability: extract_text
                        adapter:
                          kind: queue
                          queue: demo.extract
                    """
                ).strip(),
                encoding="utf-8",
            )

            payload = _run_cli(
                "--pipeline-dir",
                str(root),
                "deployment",
                "--format",
                "docker-compose",
                "--run-id",
                "run_deploy",
                "--package-id",
                "demo_package",
                "--image",
                "example/fala:test",
                "--worker-image",
                "example/fala-worker:test",
                "--with-postgres",
                "--env",
                "FALA_API_KEYS=operator-secret:operator,worker-secret:worker",
                "--worker-env",
                "FALA_API_KEY=worker-secret",
            )

            self.assertTrue(payload["ok"])
            self.assertEqual(payload["base_url"], "http://fala-control-plane:8000")
            self.assertEqual(payload["worker_count"], 1)
            compose = yaml.safe_load(payload["manifest"])
            services = compose["services"]
            self.assertIn("fala-control-plane", services)
            self.assertIn("fala-postgres", services)
            self.assertIn("fala_demo_package_extract_worker", services)

            control_plane = services["fala-control-plane"]
            self.assertEqual(control_plane["image"], "example/fala:test")
            self.assertEqual(control_plane["command"][0], "fala")
            self.assertIn("serve", control_plane["command"])
            self.assertEqual(control_plane["ports"], ["8000:8000"])
            self.assertEqual(control_plane["depends_on"], ["fala-postgres"])
            self.assertEqual(
                control_plane["environment"]["FALA_PIPELINE_DIR"],
                "/app/pipelines",
            )
            self.assertIn(
                "fala-postgres",
                control_plane["environment"]["FALA_DATABASE_URL"],
            )
            self.assertEqual(
                control_plane["environment"]["FALA_QUEUE_DB"],
                "/data/queue.sqlite",
            )
            self.assertEqual(
                control_plane["environment"]["FALA_ARTIFACT_STORE_ROOT"],
                "/data/artifact-store",
            )
            self.assertIn("fala-data:/data", control_plane["volumes"])
            self.assertIn(
                f"{root.resolve()}:/app/pipelines:ro",
                control_plane["volumes"],
            )

            worker = services["fala_demo_package_extract_worker"]
            self.assertEqual(worker["image"], "example/fala-worker:test")
            self.assertEqual(worker["depends_on"], ["fala-control-plane"])
            self.assertEqual(
                worker["command"][worker["command"].index("--base-url") + 1],
                "http://fala-control-plane:8000",
            )
            self.assertEqual(
                worker["command"][worker["command"].index("--pipeline-dir") + 1],
                "/app/pipelines",
            )
            self.assertEqual(worker["working_dir"], "/app/pipelines/demo_package")
            self.assertEqual(worker["environment"]["FALA_API_KEY"], "worker-secret")
            self.assertEqual(
                worker["environment"]["FALA_ARTIFACT_STORE_ROOT"],
                "/data/artifact-store",
            )
            self.assertEqual(
                worker["environment"]["PROCESS_RUNTIME_ARTIFACT_ROOT"],
                "/data/process-artifacts",
            )
            self.assertEqual(
                worker["environment"]["OPENAI_API_KEY"],
                "${OPENAI_API_KEY:?Fala secret openai_api_key is required}",
            )
            self.assertIn("fala-data:/data", worker["volumes"])
            self.assertIn(f"{root.resolve()}:/app/pipelines:ro", worker["volumes"])
            self.assertIn("fala-data", compose["volumes"])
            self.assertIn("fala-postgres-data", compose["volumes"])

    def test_deployment_renders_generic_queue_broker_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = _run_cli(
                "--pipeline-dir",
                str(root),
                "deployment",
                "--format",
                "docker-compose",
                "--no-workers",
                "--queue-broker",
                "memory://deploy-preview",
            )

            self.assertTrue(payload["ok"])
            compose = yaml.safe_load(payload["manifest"])
            control_plane = compose["services"]["fala-control-plane"]
            self.assertEqual(
                control_plane["environment"]["FALA_QUEUE_BROKER"],
                "memory://deploy-preview",
            )
            self.assertNotIn("FALA_QUEUE_DB", control_plane["environment"])

    def test_deployment_renders_remote_artifact_store_to_workers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "demo_package"
            package_dir.mkdir()
            (package_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: demo_package
                    capabilities:
                      - id: extract_text
                    pipelines:
                      - demo.yaml
                    workers:
                      - id: extract_worker
                        capabilities: [extract_text]
                        pipeline: demo_flow
                        process: extract
                        command: ["python", "steps/extract.py"]
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
                      - id: extract
                        capability: extract_text
                        adapter:
                          kind: queue
                          queue: demo.extract
                    """
                ).strip(),
                encoding="utf-8",
            )

            payload = _run_cli(
                "--pipeline-dir",
                str(root),
                "deployment",
                "--format",
                "docker-compose",
                "--run-id",
                "run_deploy",
                "--package-id",
                "demo_package",
                "--artifact-store",
                "s3://fala-artifacts/runtime",
                "--artifact-cache-root",
                "/data/artifact-cache",
            )

            self.assertTrue(payload["ok"])
            compose = yaml.safe_load(payload["manifest"])
            control_plane = compose["services"]["fala-control-plane"]
            worker = compose["services"]["fala_demo_package_extract_worker"]
            self.assertEqual(
                control_plane["environment"]["FALA_ARTIFACT_STORE"],
                "s3://fala-artifacts/runtime",
            )
            self.assertNotIn("FALA_ARTIFACT_STORE_ROOT", control_plane["environment"])
            self.assertEqual(
                worker["environment"]["FALA_ARTIFACT_STORE"],
                "s3://fala-artifacts/runtime",
            )
            self.assertEqual(
                worker["environment"]["FALA_ARTIFACT_CACHE_ROOT"],
                "/data/artifact-cache",
            )
            self.assertNotIn("FALA_ARTIFACT_STORE_ROOT", worker["environment"])

    def test_deployment_renders_kubernetes_control_plane_and_workers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "demo_package"
            package_dir.mkdir()
            (package_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: demo_package
                    capabilities:
                      - id: extract_text
                    pipelines:
                      - demo.yaml
                    workers:
                      - id: extract_worker
                        capabilities: [extract_text]
                        pipeline: demo_flow
                        process: extract
                        command: ["python", "steps/extract.py"]
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
                      - id: extract
                        capability: extract_text
                        adapter:
                          kind: queue
                          queue: demo.extract
                    """
                ).strip(),
                encoding="utf-8",
            )

            payload = _run_cli(
                "--pipeline-dir",
                str(root),
                "deployment",
                "--format",
                "kubernetes",
                "--run-id",
                "run_deploy",
                "--package-id",
                "demo_package",
                "--image",
                "example/fala:test",
                "--worker-image",
                "example/fala-worker:test",
                "--namespace",
                "fala",
                "--control-plane-replicas",
                "2",
                "--worker-replicas",
                "3",
                "--database-url",
                "postgresql://postgres/fala",
            )

            manifests = list(yaml.safe_load_all(payload["manifest"]))
            by_name = {
                (manifest["kind"], manifest["metadata"]["name"]): manifest
                for manifest in manifests
            }
            self.assertIn(("PersistentVolumeClaim", "fala-data"), by_name)
            control_plane = by_name[("Deployment", "fala-control-plane")]
            self.assertEqual(control_plane["metadata"]["namespace"], "fala")
            self.assertEqual(control_plane["spec"]["replicas"], 2)
            control_container = control_plane["spec"]["template"]["spec"]["containers"][0]
            self.assertEqual(control_container["image"], "example/fala:test")
            env_by_name = {item["name"]: item["value"] for item in control_container["env"]}
            self.assertEqual(env_by_name["FALA_PIPELINE_DIR"], "/app/pipelines")
            self.assertEqual(env_by_name["FALA_DATABASE_URL"], "postgresql://postgres/fala")
            self.assertEqual(
                control_container["volumeMounts"],
                [{"name": "fala-data", "mountPath": "/data"}],
            )
            self.assertIn(("Service", "fala-control-plane"), by_name)

            worker = by_name[("Deployment", "fala-demo-package-extract-worker")]
            self.assertEqual(worker["metadata"]["namespace"], "fala")
            self.assertEqual(worker["spec"]["replicas"], 3)
            worker_container = worker["spec"]["template"]["spec"]["containers"][0]
            self.assertEqual(worker_container["image"], "example/fala-worker:test")
            self.assertEqual(
                worker_container["args"][worker_container["args"].index("--base-url") + 1],
                "http://fala-control-plane:8000",
            )
            self.assertEqual(
                worker_container["args"][worker_container["args"].index("--pipeline-dir") + 1],
                "/app/pipelines",
            )
            self.assertEqual(worker_container["workingDir"], "/app/pipelines/demo_package")
            worker_env = {item["name"]: item["value"] for item in worker_container["env"]}
            self.assertEqual(worker_env["FALA_ARTIFACT_STORE_ROOT"], "/data/artifact-store")
            self.assertEqual(worker_env["PROCESS_RUNTIME_ARTIFACT_ROOT"], "/data/process-artifacts")
            self.assertEqual(
                worker_container["volumeMounts"],
                [{"name": "fala-data", "mountPath": "/data"}],
            )

    def test_worker_deployment_renders_compose_and_kubernetes_manifests(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "demo_package"
            package_dir.mkdir()
            (package_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: demo_package
                    capabilities:
                      - id: extract_text
                    secrets:
                      - id: openai_api_key
                        env_var: OPENAI_API_KEY
                        kubernetes_secret_name: fala-openai
                        kubernetes_secret_key: api-key
                    pipelines:
                      - demo.yaml
                    workers:
                      - id: extract_worker
                        capabilities: [extract_text]
                        pipeline: demo_flow
                        process: extract
                        command: ["python", "steps/extract.py"]
                        cwd: "/app"
                        env:
                          OCR_MODE: fast
                        secrets: [openai_api_key]
                        sandbox:
                          run_as_non_root: true
                          read_only_root_filesystem: true
                          allow_privilege_escalation: false
                          drop_capabilities: [ALL]
                        resources:
                          cpu_cores: 1
                          memory_mb: 768
                          gpu_count: 1
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "demo.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: demo_flow
                    steps:
                      - id: extract
                        capability: extract_text
                        adapter:
                          kind: queue
                          queue: demo.extract
                    """
                ).strip(),
                encoding="utf-8",
            )

            compose_payload = _run_cli(
                "--pipeline-dir",
                str(root),
                "worker-deployment",
                "--base-url",
                "http://runtime.test",
                "--run-id",
                "run_deploy",
                "--package-id",
                "demo_package",
                "--format",
                "docker-compose",
                "--image",
                "example/fala-worker:test",
                "--env",
                "FALA_DATABASE_URL=postgresql://db/fala",
            )
            self.assertTrue(compose_payload["ok"])
            compose = yaml.safe_load(compose_payload["manifest"])
            service = compose["services"]["fala_demo_package_extract_worker"]
            self.assertEqual(service["image"], "example/fala-worker:test")
            self.assertIn("--package-worker", service["command"])
            self.assertIn("extract_worker", service["command"])
            self.assertEqual(service["working_dir"], "/app")
            self.assertEqual(service["environment"]["OCR_MODE"], "fast")
            self.assertEqual(
                service["environment"]["FALA_DATABASE_URL"],
                "postgresql://db/fala",
            )
            self.assertEqual(
                service["environment"]["OPENAI_API_KEY"],
                "${OPENAI_API_KEY:?Fala secret openai_api_key is required}",
            )
            self.assertEqual(
                service["x-fala"]["resources"]["memory_mb"],
                768,
            )
            self.assertEqual(
                service["x-fala"]["secrets"][0]["env_var"],
                "OPENAI_API_KEY",
            )

            k8s_payload = _run_cli(
                "--pipeline-dir",
                str(root),
                "worker-deployment",
                "--base-url",
                "http://runtime.test",
                "--run-id",
                "run_deploy",
                "--package-id",
                "demo_package",
                "--format",
                "kubernetes",
                "--image",
                "example/fala-worker:test",
                "--replicas",
                "2",
                "--namespace",
                "fala",
            )
            deployment = yaml.safe_load(k8s_payload["manifest"])
            self.assertEqual(deployment["kind"], "Deployment")
            self.assertEqual(deployment["metadata"]["name"], "fala-demo-package-extract-worker")
            self.assertEqual(deployment["metadata"]["namespace"], "fala")
            self.assertEqual(deployment["spec"]["replicas"], 2)
            container = deployment["spec"]["template"]["spec"]["containers"][0]
            self.assertEqual(container["image"], "example/fala-worker:test")
            env_by_name = {item["name"]: item for item in container["env"]}
            self.assertEqual(env_by_name["OCR_MODE"]["value"], "fast")
            self.assertEqual(
                env_by_name["OPENAI_API_KEY"]["valueFrom"]["secretKeyRef"],
                {
                    "name": "fala-openai",
                    "key": "api-key",
                    "optional": False,
                },
            )
            self.assertEqual(
                container["securityContext"]["readOnlyRootFilesystem"],
                True,
            )
            self.assertEqual(
                container["securityContext"]["allowPrivilegeEscalation"],
                False,
            )
            self.assertEqual(
                container["securityContext"]["capabilities"]["drop"],
                ["ALL"],
            )
            self.assertEqual(container["resources"]["requests"]["memory"], "768Mi")
            self.assertEqual(container["resources"]["requests"]["nvidia.com/gpu"], "1")
            self.assertEqual(container["resources"]["limits"]["nvidia.com/gpu"], "1")

            keda_payload = _run_cli(
                "--pipeline-dir",
                str(root),
                "worker-autoscaling",
                "--base-url",
                "http://runtime.test",
                "--run-id",
                "run_deploy",
                "--package-id",
                "demo_package",
                "--prometheus-server",
                "http://prometheus:9090",
                "--max-replicas",
                "7",
                "--namespace",
                "fala",
            )
            scaled_object = yaml.safe_load(keda_payload["manifest"])
            self.assertEqual(scaled_object["kind"], "ScaledObject")
            self.assertEqual(
                scaled_object["metadata"]["name"],
                "fala-demo-package-extract-worker-autoscale",
            )
            self.assertEqual(scaled_object["metadata"]["namespace"], "fala")
            self.assertEqual(
                scaled_object["spec"]["scaleTargetRef"]["name"],
                "fala-demo-package-extract-worker",
            )
            self.assertEqual(scaled_object["spec"]["maxReplicaCount"], 7)
            trigger = scaled_object["spec"]["triggers"][0]
            self.assertEqual(trigger["type"], "prometheus")
            self.assertEqual(
                trigger["metadata"]["serverAddress"],
                "http://prometheus:9090",
            )
            self.assertIn('run_id="run_deploy"', trigger["metadata"]["query"])
            self.assertIn(
                'package_worker_id="extract_worker"',
                trigger["metadata"]["query"],
            )

    def test_worker_deployment_maps_host_package_paths_to_container_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "demo_package"
            package_dir.mkdir()
            (package_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: demo_package
                    capabilities:
                      - id: extract_text
                    pipelines:
                      - demo.yaml
                    workers:
                      - id: extract_worker
                        capabilities: [extract_text]
                        pipeline: demo_flow
                        process: extract
                        command: ["python", "steps/extract.py"]
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
                      - id: extract
                        capability: extract_text
                        adapter:
                          kind: queue
                          queue: demo.extract
                    """
                ).strip(),
                encoding="utf-8",
            )

            compose_payload = _run_cli(
                "--pipeline-dir",
                str(root),
                "worker-deployment",
                "--base-url",
                "http://runtime.test",
                "--run-id",
                "run_deploy",
                "--package-id",
                "demo_package",
                "--format",
                "docker-compose",
                "--image",
                "example/fala-worker:test",
                "--container-pipeline-dir",
                "/app/pipelines",
            )
            compose = yaml.safe_load(compose_payload["manifest"])
            service = compose["services"]["fala_demo_package_extract_worker"]
            self.assertEqual(
                service["command"][
                    service["command"].index("--pipeline-dir") + 1
                ],
                "/app/pipelines",
            )
            self.assertEqual(service["working_dir"], "/app/pipelines/demo_package")
            self.assertEqual(
                service["volumes"],
                [f"{root.resolve()}:/app/pipelines:ro"],
            )
            self.assertNotIn(str(root), " ".join(str(item) for item in service["command"]))
            self.assertNotIn(str(root), service["working_dir"])

            k8s_payload = _run_cli(
                "--pipeline-dir",
                str(root),
                "worker-deployment",
                "--base-url",
                "http://runtime.test",
                "--run-id",
                "run_deploy",
                "--package-id",
                "demo_package",
                "--format",
                "kubernetes",
                "--image",
                "example/fala-worker:test",
                "--container-pipeline-dir",
                "/app/pipelines",
            )
            deployment = yaml.safe_load(k8s_payload["manifest"])
            container = deployment["spec"]["template"]["spec"]["containers"][0]
            self.assertEqual(
                container["args"][container["args"].index("--pipeline-dir") + 1],
                "/app/pipelines",
            )
            self.assertEqual(container["workingDir"], "/app/pipelines/demo_package")
            self.assertNotIn(str(root), k8s_payload["manifest"])

    def test_worker_deployment_can_skip_compose_pipeline_mount(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "demo_package"
            package_dir.mkdir()
            (package_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: demo_package
                    capabilities:
                      - id: extract_text
                    pipelines:
                      - demo.yaml
                    workers:
                      - id: extract_worker
                        capabilities: [extract_text]
                        pipeline: demo_flow
                        process: extract
                        command: ["python", "steps/extract.py"]
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
                      - id: extract
                        capability: extract_text
                        adapter:
                          kind: queue
                          queue: demo.extract
                    """
                ).strip(),
                encoding="utf-8",
            )

            payload = _run_cli(
                "--pipeline-dir",
                str(root),
                "worker-deployment",
                "--base-url",
                "http://runtime.test",
                "--run-id",
                "run_deploy",
                "--package-id",
                "demo_package",
                "--format",
                "docker-compose",
                "--image",
                "example/fala-worker:test",
                "--container-pipeline-dir",
                "/app/pipelines",
                "--no-mount-pipeline-dir",
            )
            compose = yaml.safe_load(payload["manifest"])
            service = compose["services"]["fala_demo_package_extract_worker"]
            self.assertNotIn("volumes", service)
            self.assertEqual(service["working_dir"], "/app/pipelines/demo_package")

    def test_worker_deployment_container_workdir_overrides_mapped_cwd(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "demo_package"
            package_dir.mkdir()
            (package_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: demo_package
                    capabilities:
                      - id: extract_text
                    pipelines:
                      - demo.yaml
                    workers:
                      - id: extract_worker
                        capabilities: [extract_text]
                        pipeline: demo_flow
                        process: extract
                        command: ["python", "steps/extract.py"]
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
                      - id: extract
                        capability: extract_text
                        adapter:
                          kind: queue
                          queue: demo.extract
                    """
                ).strip(),
                encoding="utf-8",
            )

            payload = _run_cli(
                "--pipeline-dir",
                str(root),
                "worker-deployment",
                "--base-url",
                "http://runtime.test",
                "--run-id",
                "run_deploy",
                "--package-id",
                "demo_package",
                "--format",
                "docker-compose",
                "--image",
                "example/fala-worker:test",
                "--container-pipeline-dir",
                "/app/pipelines",
                "--container-workdir",
                "/workspace",
            )
            compose = yaml.safe_load(payload["manifest"])
            service = compose["services"]["fala_demo_package_extract_worker"]
            self.assertEqual(service["working_dir"], "/workspace")

    def test_process_supervisor_restarts_failed_worker(self) -> None:
        result = asyncio.run(
            ProcessSupervisor(
                [
                    SupervisedWorkerSpec(
                        id="failing_worker",
                        argv=[
                            sys.executable,
                            "-c",
                            "import sys; sys.exit(2)",
                        ],
                    )
                ],
                restart_policy="on-failure",
                max_restarts=1,
                restart_delay_seconds=0,
            ).run()
        )

        self.assertFalse(result.ok)
        self.assertEqual(result.worker_count, 1)
        worker = result.workers[0]
        self.assertEqual(worker.status, "failed")
        self.assertEqual(worker.exit_code, 2)
        self.assertEqual(worker.starts, 2)
        self.assertEqual(worker.restarts, 1)

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
        lineage = result.outputs["parse_metadata"].metadata["process_runtime"]["lineage"]
        self.assertEqual(lineage["process_id"], "parse_metadata")
        self.assertEqual(lineage["needs"], ["extract_text"])
        self.assertEqual(lineage["needs_value_keys"], {"extract_text": ["text"]})
        self.assertEqual(
            lineage["dependency_outputs"][0]["process_id"],
            "extract_text",
        )
        self.assertEqual(
            lineage["dependency_outputs"][0]["value_keys"],
            ["text"],
        )
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
                    work_items:
                      claim_strategy: sequential
                      order_by: priority
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
            self.assertEqual(spec.work_items.claim_strategy, "sequential")
            self.assertEqual(spec.work_items.order_by, "priority")
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
            (root / "document-values.yaml").write_text(
                textwrap.dedent(
                    """
                    type: object
                    properties:
                      case_id:
                        type: string
                    additionalProperties: true
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
                    capability="enrich_document",
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
        self.assertEqual(scheduled.queued[0].capability, "enrich_document")
        statuses = asyncio.run(
            store.list_statuses(run_id="run_schedule", document_id="doc_schedule")
        )
        self.assertEqual(statuses["first"].value, "completed")
        self.assertEqual(statuses["second"].value, "queued")

        skipped_claim = asyncio.run(
            scheduler.claim_next(
                run_id="run_schedule",
                document_ids=["doc_schedule"],
                worker_id="worker-ctx",
                adapter_kind="queue",
                capabilities=["render_document"],
            )
        )
        self.assertIsNone(skipped_claim)

        claim = asyncio.run(
            scheduler.claim_next(
                run_id="run_schedule",
                document_ids=["doc_schedule"],
                worker_id="worker-ctx",
                adapter_kind="queue",
                capabilities=["enrich_document"],
            )
        )

        self.assertIsNotNone(claim)
        assert claim is not None
        self.assertEqual(claim.process.id, "second")
        self.assertEqual(claim.process.capability, "enrich_document")
        self.assertEqual(claim.context.process_id, "second")
        self.assertEqual(claim.context.capability, "enrich_document")
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

    def test_scheduler_claims_higher_priority_process_first(self) -> None:
        store = InMemoryStateStore()
        pipeline = PipelineSpec(
            id="priority_claim",
            steps=[
                ProcessSpec(
                    id="cheap_index",
                    priority=0,
                    adapter=AdapterSpec(kind="queue", queue="docs.index"),
                ),
                ProcessSpec(
                    id="expensive_ocr",
                    priority=50,
                    adapter=AdapterSpec(kind="queue", queue="docs.ocr"),
                ),
            ],
        )
        scheduler = PipelineScheduler(pipeline, store)
        asyncio.run(
            scheduler.initialize_document(
                run_id="run_priority",
                document_id="doc_priority",
                values={},
            )
        )

        claim = asyncio.run(
            scheduler.claim_next(
                run_id="run_priority",
                document_ids=["doc_priority"],
                worker_id="worker-priority",
                adapter_kind="queue",
            )
        )

        self.assertIsNotNone(claim)
        assert claim is not None
        self.assertEqual(claim.process.id, "expensive_ocr")
        self.assertEqual(claim.process.priority, 50)
        events = asyncio.run(
            store.list_events(
                run_id="run_priority",
                document_id="doc_priority",
                process_id="expensive_ocr",
            )
        )
        self.assertEqual(events[-1].type, "process.claimed")
        self.assertEqual(events[-1].data["priority"], 50)

    def test_scheduler_respects_process_max_concurrency_across_documents(self) -> None:
        store = InMemoryStateStore()
        pipeline = PipelineSpec(
            id="limited_claim",
            steps=[
                ProcessSpec(
                    id="ocr",
                    max_concurrency=1,
                    adapter=AdapterSpec(kind="queue", queue="docs.ocr"),
                )
            ],
        )
        scheduler = PipelineScheduler(pipeline, store)
        for document_id in ["doc_a", "doc_b"]:
            asyncio.run(
                scheduler.initialize_document(
                    run_id="run_limited",
                    document_id=document_id,
                    values={},
                )
            )

        first = asyncio.run(
            scheduler.claim_next(
                run_id="run_limited",
                document_ids=["doc_a", "doc_b"],
                worker_id="worker-a",
                adapter_kind="queue",
            )
        )
        second = asyncio.run(
            scheduler.claim_next(
                run_id="run_limited",
                document_ids=["doc_a", "doc_b"],
                worker_id="worker-b",
                adapter_kind="queue",
            )
        )

        self.assertIsNotNone(first)
        assert first is not None
        self.assertEqual(first.document_id, "doc_a")
        self.assertEqual(first.process.max_concurrency, 1)
        self.assertIsNone(second)
        statuses_b = asyncio.run(
            store.list_statuses(run_id="run_limited", document_id="doc_b")
        )
        self.assertEqual(statuses_b["ocr"], ProcessStatus.queued)

        asyncio.run(
            store.put_output(
                run_id="run_limited",
                document_id="doc_a",
                process_id="ocr",
                output=ProcessOutput(values={"ok": True}),
            )
        )
        third = asyncio.run(
            scheduler.claim_next(
                run_id="run_limited",
                document_ids=["doc_a", "doc_b"],
                worker_id="worker-b",
                adapter_kind="queue",
            )
        )

        self.assertIsNotNone(third)
        assert third is not None
        self.assertEqual(third.document_id, "doc_b")

    def test_scheduler_claims_work_items_in_parallel_by_default(self) -> None:
        store = InMemoryStateStore()
        pipeline = PipelineSpec(
            id="parallel_claim_test",
            steps=[
                ProcessSpec(
                    id="process",
                    adapter=AdapterSpec(kind="queue", queue="test.process"),
                )
            ],
        )
        scheduler = PipelineScheduler(pipeline, store)
        for index, document_id in enumerate(("z-first.item", "a-second.item"), start=1):
            asyncio.run(
                scheduler.initialize_document(
                    run_id="run_parallel_claim",
                    document_id=document_id,
                    values={"index": index},
                )
            )

        first_claim = asyncio.run(
            scheduler.claim_next(
                run_id="run_parallel_claim",
                document_ids=["z-first.item", "a-second.item"],
                worker_id="worker-1",
                adapter_kind="queue",
            )
        )
        second_claim = asyncio.run(
            scheduler.claim_next(
                run_id="run_parallel_claim",
                document_ids=["z-first.item", "a-second.item"],
                worker_id="worker-2",
                adapter_kind="queue",
            )
        )

        self.assertIsNotNone(first_claim)
        self.assertIsNotNone(second_claim)
        assert first_claim is not None
        assert second_claim is not None
        self.assertNotEqual(first_claim.document_id, second_claim.document_id)

    def test_scheduler_claims_work_items_sequentially_by_input_order(self) -> None:
        store = InMemoryStateStore()
        pipeline = PipelineSpec(
            id="sequential_claim_test",
            work_items=WorkItemPolicy(claim_strategy="sequential", order_by="index"),
            steps=[
                ProcessSpec(
                    id="process",
                    adapter=AdapterSpec(kind="queue", queue="test.process"),
                )
            ],
        )
        scheduler = PipelineScheduler(pipeline, store)
        for index, document_id in enumerate(("z-first.item", "a-second.item"), start=1):
            asyncio.run(
                scheduler.initialize_document(
                    run_id="run_sequential_claim",
                    document_id=document_id,
                    values={"index": index},
                )
            )

        first_claim = asyncio.run(
            scheduler.claim_next(
                run_id="run_sequential_claim",
                document_ids=["z-first.item", "a-second.item"],
                worker_id="worker-1",
                adapter_kind="queue",
            )
        )
        self.assertIsNotNone(first_claim)
        assert first_claim is not None
        self.assertEqual(first_claim.document_id, "z-first.item")

        blocked_claim = asyncio.run(
            scheduler.claim_next(
                run_id="run_sequential_claim",
                document_ids=["z-first.item", "a-second.item"],
                worker_id="worker-2",
                adapter_kind="queue",
            )
        )
        self.assertIsNone(blocked_claim)

        asyncio.run(
            store.put_output(
                run_id="run_sequential_claim",
                document_id="z-first.item",
                process_id="process",
                output=ProcessOutput(values={"ok": True}),
            )
        )
        second_claim = asyncio.run(
            scheduler.claim_next(
                run_id="run_sequential_claim",
                document_ids=["z-first.item", "a-second.item"],
                worker_id="worker-3",
                adapter_kind="queue",
            )
        )

        self.assertIsNotNone(second_claim)
        assert second_claim is not None
        self.assertEqual(second_claim.document_id, "a-second.item")

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

    def test_postgres_state_store_is_optional_and_matches_store_surface(self) -> None:
        store = PostgresStateStore(
            "postgresql://fala:secret@db/fala",
            ensure_schema=False,
        )
        self.assertEqual(store.dsn, "postgresql://fala:secret@db/fala")

        expected_methods = [
            name
            for name, value in StateStore.__dict__.items()
            if name.startswith("_") is False and callable(value)
        ]
        missing = [
            name
            for name in expected_methods
            if not callable(getattr(PostgresStateStore, name, None))
        ]
        self.assertEqual(missing, [])

    def test_state_store_factory_defaults_to_sqlite_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "runtime.db"
            store = create_state_store(db_path)
            self.assertIsInstance(store, SQLiteStateStore)
            self.assertEqual(store.path, db_path)

    def test_state_store_factory_selects_postgres_for_dsn(self) -> None:
        store = create_state_store(
            "postgresql://fala:secret@db/fala",
            ensure_schema=False,
        )
        self.assertIsInstance(store, PostgresStateStore)
        self.assertEqual(store.dsn, "postgresql://fala:secret@db/fala")

    def test_state_store_factory_reads_runtime_database_environment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "env-runtime.db"
            with patch.dict("os.environ", {"FALA_DATABASE_URL": str(db_path)}, clear=True):
                self.assertEqual(default_state_store_target(), str(db_path))
                store = create_state_store(default_state_store_target())
            self.assertIsInstance(store, SQLiteStateStore)
            self.assertEqual(store.path, db_path)

    def test_runtime_db_diagnostics_reports_schema_migration_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "runtime.db"
            initialized = _run_cli(
                "db-doctor",
                "--db",
                str(db_path),
                "--ensure-schema",
            )
            self.assertTrue(initialized["ok"])
            self.assertEqual(
                initialized["schema"]["current_version"],
                RUNTIME_SCHEMA_VERSION,
            )
            self.assertEqual(
                initialized["schema"]["latest_version"],
                RUNTIME_SCHEMA_VERSION,
            )
            self.assertTrue(initialized["schema"]["migrations"]["ok"])
            self.assertEqual(initialized["schema"]["migrations"]["missing_count"], 0)
            with closing(sqlite3.connect(db_path)) as conn:
                row = conn.execute("PRAGMA user_version").fetchone()
                self.assertIsNotNone(row)
                assert row is not None
                self.assertEqual(int(row[0]), RUNTIME_SCHEMA_VERSION)
                migration_count = conn.execute(
                    "SELECT COUNT(*) FROM runtime_schema_migrations"
                ).fetchone()[0]
            self.assertEqual(migration_count, RUNTIME_SCHEMA_VERSION)

    def test_runtime_db_diagnostics_flags_unversioned_sqlite_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "legacy.db"
            with closing(sqlite3.connect(db_path)) as conn:
                conn.execute(
                    """
                    CREATE TABLE process_runs (
                      id TEXT PRIMARY KEY,
                      status TEXT NOT NULL,
                      title TEXT,
                      outcome TEXT,
                      outcome_reason TEXT,
                      config TEXT NOT NULL,
                      metadata TEXT NOT NULL,
                      summary TEXT NOT NULL,
                      created_at TEXT NOT NULL,
                      updated_at TEXT NOT NULL,
                      started_at TEXT,
                      finished_at TEXT,
                      payload TEXT NOT NULL
                    )
                    """
                )
            code, legacy = _run_cli_raw("db-doctor", "--db", str(db_path))
            self.assertEqual(code, 1)
            self.assertFalse(legacy["ok"])
            self.assertIn("runtime_schema_migrations", legacy["schema"]["missing_tables"])
            self.assertEqual(legacy["schema"]["current_version"], 0)
            self.assertFalse(legacy["schema"]["migrations"]["ok"])
            self.assertEqual(
                legacy["schema"]["migrations"]["missing_count"],
                RUNTIME_SCHEMA_VERSION,
            )

    def test_postgres_state_store_schema_covers_runtime_tables(self) -> None:
        for table in [
            "runtime_schema_migrations",
            "process_runs",
            "operator_audit_events",
            "process_worker_heartbeats",
            "process_documents",
            "process_events",
            "process_statuses",
            "process_document_inputs",
            "process_claims",
            "process_outputs",
            "process_stream_chunks",
            "process_stream_checkpoints",
            "process_projections",
        ]:
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table}", POSTGRES_SCHEMA_SQL)

        for index in [
            "idx_process_events_run_doc_process_ts_id",
            "idx_process_statuses_run_status",
            "idx_process_claims_run_doc_expires",
            "idx_process_stream_chunks_lookup",
        ]:
            self.assertIn(index, POSTGRES_SCHEMA_SQL)

    def test_postgres_claim_sql_is_atomic(self) -> None:
        sql = " ".join(POSTGRES_TRY_CLAIM_STATUS_SQL.split())
        self.assertIn("UPDATE process_statuses SET status = %s", sql)
        self.assertIn("AND status = %s", sql)
        self.assertIn("AND NOT EXISTS", sql)
        self.assertIn("FROM process_outputs", sql)
        self.assertIn("RETURNING process_id", sql)

    def test_postgres_state_store_live_runtime_contract_when_configured(self) -> None:
        dsn = os.environ.get("FALA_POSTGRES_TEST_DSN")
        if not dsn:
            self.skipTest("FALA_POSTGRES_TEST_DSN is not set")
        try:
            import psycopg  # noqa: F401
        except ImportError:
            self.skipTest("psycopg is not installed")

        run_id = f"run_pg_{uuid.uuid4().hex}"
        document_id = "doc_pg"
        pipeline = PipelineSpec(
            id="postgres_live_contract",
            steps=[
                ProcessSpec(
                    id="extract",
                    adapter=AdapterSpec(kind="queue", queue="postgres.extract"),
                )
            ],
        )
        registry = PipelineRegistry([pipeline])

        async def exercise() -> tuple[list[object | None], RuntimeStreamChunk]:
            store = create_state_store(dsn)
            service = RuntimeService(registry=registry, store=store)
            run, schedules = await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id=run_id,
                    pipeline_id=pipeline.id,
                    documents=[
                        RuntimeDocumentInput(
                            document_id=document_id,
                            values={"source": "postgres-live-test"},
                        )
                    ],
                )
            )
            self.assertEqual(run.id, run_id)
            self.assertEqual(len(schedules), 1)

            scheduler_1 = PipelineScheduler(pipeline, create_state_store(dsn))
            scheduler_2 = PipelineScheduler(pipeline, create_state_store(dsn))
            claims = await asyncio.gather(
                scheduler_1.claim_next(
                    run_id=run_id,
                    document_ids=[document_id],
                    worker_id="worker-1",
                    lease_seconds=120,
                ),
                scheduler_2.claim_next(
                    run_id=run_id,
                    document_ids=[document_id],
                    worker_id="worker-2",
                    lease_seconds=120,
                ),
            )

            chunk = await service.append_stream_chunk(
                run_id=run_id,
                document_id=document_id,
                process_id="extract",
                pipeline_id=pipeline.id,
                stream_id="pages",
                values={"text": "hello"},
            )
            checkpoint = await service.put_stream_checkpoint(
                run_id=run_id,
                document_id=document_id,
                process_id="extract",
                stream_id="pages",
                consumer_id="indexer",
                sequence=chunk.sequence,
                chunk_id=chunk.chunk_id,
            )
            loaded = await service.get_stream_checkpoint(
                run_id=run_id,
                document_id=document_id,
                process_id="extract",
                stream_id="pages",
                consumer_id="indexer",
            )
            self.assertEqual(loaded, checkpoint)
            return claims, chunk

        try:
            claims, chunk = asyncio.run(exercise())
        finally:
            cleanup_store = create_state_store(dsn)
            asyncio.run(cleanup_store.delete_run(run_id))

        claimed = [claim for claim in claims if claim is not None]
        self.assertEqual(len(claimed), 1)
        self.assertIn(claimed[0].worker_id, {"worker-1", "worker-2"})
        self.assertEqual(chunk.sequence, 0)
        self.assertEqual(chunk.values["text"], "hello")

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

    def test_scheduler_respects_retry_delay_after_expired_claim(self) -> None:
        store = InMemoryStateStore()
        pipeline = PipelineSpec(
            id="retry_claim_delay_test",
            steps=[
                ProcessSpec(
                    id="extract",
                    adapter=AdapterSpec(kind="queue", queue="test.extract"),
                    retry=RetryPolicy(max_attempts=2, delay_seconds=0.02),
                )
            ],
        )
        scheduler = PipelineScheduler(pipeline, store)

        async def exercise() -> None:
            await scheduler.initialize_document(
                run_id="run_retry_claim_delay",
                document_id="doc_retry_claim_delay",
                values={},
            )
            first = await scheduler.claim_next(
                run_id="run_retry_claim_delay",
                document_ids=["doc_retry_claim_delay"],
                worker_id="worker-1",
                lease_seconds=0,
            )
            self.assertIsNotNone(first)
            assert first is not None
            self.assertEqual(first.attempt, 1)

            blocked = await scheduler.claim_next(
                run_id="run_retry_claim_delay",
                document_ids=["doc_retry_claim_delay"],
                worker_id="worker-2",
            )
            self.assertIsNone(blocked)
            statuses = await store.list_statuses(
                run_id="run_retry_claim_delay",
                document_id="doc_retry_claim_delay",
            )
            self.assertEqual(statuses["extract"], ProcessStatus.waiting)
            events = await store.list_events(
                run_id="run_retry_claim_delay",
                document_id="doc_retry_claim_delay",
                process_id="extract",
            )
            retry_event = next(event for event in events if event.type == "process.claim_expired")
            self.assertEqual(retry_event.status, ProcessStatus.waiting)
            self.assertEqual(retry_event.data["next_status"], "waiting")
            self.assertIn("retry_after", retry_event.data)

            await asyncio.sleep(0.03)
            second = await scheduler.claim_next(
                run_id="run_retry_claim_delay",
                document_ids=["doc_retry_claim_delay"],
                worker_id="worker-2",
            )
            self.assertIsNotNone(second)
            assert second is not None
            self.assertEqual(second.attempt, 2)

        asyncio.run(exercise())

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

    def test_scheduler_classifies_failure_error_kinds_for_retry(self) -> None:
        store = InMemoryStateStore()
        pipeline = PipelineSpec(
            id="retry_kind_test",
            steps=[
                ProcessSpec(
                    id="extract",
                    adapter=AdapterSpec(kind="queue", queue="test.extract"),
                    retry=RetryPolicy(
                        max_attempts=3,
                        retry_error_kinds=["transient_io"],
                        terminal_error_kinds=["validation_error"],
                    ),
                )
            ],
        )
        scheduler = PipelineScheduler(pipeline, store)

        async def exercise() -> None:
            await scheduler.initialize_document(
                run_id="run_retry_kind",
                document_id="doc_retry_kind",
                values={},
            )
            claim = await scheduler.claim_next(
                run_id="run_retry_kind",
                document_ids=["doc_retry_kind"],
                worker_id="worker-1",
            )
            self.assertIsNotNone(claim)
            retry = await scheduler.record_process_failure(
                run_id="run_retry_kind",
                document_id="doc_retry_kind",
                process_id="extract",
                error_kind="transient_io",
                data={"error": "temporary"},
            )
            self.assertEqual(retry.action, ProcessAction.retry)

            await scheduler.claim_next(
                run_id="run_retry_kind",
                document_ids=["doc_retry_kind"],
                worker_id="worker-2",
            )
            terminal = await scheduler.record_process_failure(
                run_id="run_retry_kind",
                document_id="doc_retry_kind",
                process_id="extract",
                error_kind="validation_error",
                data={"error": "bad input"},
            )
            self.assertEqual(terminal.action, ProcessAction.fail)
            events = await store.list_events(
                run_id="run_retry_kind",
                document_id="doc_retry_kind",
                process_id="extract",
            )
            failed = [event for event in events if event.type == "process.failed"][-1]
            self.assertEqual(failed.data["error_kind"], "validation_error")
            self.assertFalse(failed.data["retry_allowed"])
            self.assertEqual(failed.data["terminal_reason"], "terminal_error_kind")

        asyncio.run(exercise())

    def test_scheduler_fails_unknown_error_kind_when_retry_allowlist_is_set(self) -> None:
        store = InMemoryStateStore()
        pipeline = PipelineSpec(
            id="retry_kind_allowlist_test",
            steps=[
                ProcessSpec(
                    id="extract",
                    adapter=AdapterSpec(kind="queue", queue="test.extract"),
                    retry=RetryPolicy(
                        max_attempts=3,
                        retry_error_kinds=["transient_io"],
                    ),
                )
            ],
        )
        scheduler = PipelineScheduler(pipeline, store)

        async def exercise() -> None:
            await scheduler.initialize_document(
                run_id="run_retry_allowlist",
                document_id="doc_retry_allowlist",
                values={},
            )
            await scheduler.claim_next(
                run_id="run_retry_allowlist",
                document_ids=["doc_retry_allowlist"],
                worker_id="worker-1",
            )
            failure = await scheduler.record_process_failure(
                run_id="run_retry_allowlist",
                document_id="doc_retry_allowlist",
                process_id="extract",
                error_kind="permission_denied",
                data={"error": "forbidden"},
            )
            self.assertEqual(failure.action, ProcessAction.fail)
            events = await store.list_events(
                run_id="run_retry_allowlist",
                document_id="doc_retry_allowlist",
                process_id="extract",
            )
            failed = next(event for event in events if event.type == "process.failed")
            self.assertEqual(failed.data["terminal_reason"], "non_retryable_error_kind")

        asyncio.run(exercise())

    def test_scheduler_respects_retry_delay_after_process_failure(self) -> None:
        store = InMemoryStateStore()
        pipeline = PipelineSpec(
            id="retry_failure_delay_test",
            steps=[
                ProcessSpec(
                    id="extract",
                    adapter=AdapterSpec(kind="queue", queue="test.extract"),
                    retry=RetryPolicy(max_attempts=2, delay_seconds=0.02),
                )
            ],
        )
        scheduler = PipelineScheduler(pipeline, store)

        async def exercise() -> None:
            await scheduler.initialize_document(
                run_id="run_retry_failure_delay",
                document_id="doc_retry_failure_delay",
                values={},
            )
            first = await scheduler.claim_next(
                run_id="run_retry_failure_delay",
                document_ids=["doc_retry_failure_delay"],
                worker_id="worker-1",
            )
            self.assertIsNotNone(first)
            assert first is not None
            self.assertEqual(first.attempt, 1)

            first_failure = await scheduler.record_process_failure(
                run_id="run_retry_failure_delay",
                document_id="doc_retry_failure_delay",
                process_id="extract",
                data={"error": "boom"},
            )
            self.assertEqual(first_failure.action, "retry")
            self.assertEqual(first_failure.schedule.waiting, ["extract"])
            statuses = await store.list_statuses(
                run_id="run_retry_failure_delay",
                document_id="doc_retry_failure_delay",
            )
            self.assertEqual(statuses["extract"], ProcessStatus.waiting)

            blocked = await scheduler.claim_next(
                run_id="run_retry_failure_delay",
                document_ids=["doc_retry_failure_delay"],
                worker_id="worker-2",
            )
            self.assertIsNone(blocked)
            events = await store.list_events(
                run_id="run_retry_failure_delay",
                document_id="doc_retry_failure_delay",
                process_id="extract",
            )
            retry_event = next(event for event in events if event.type == "process.retry_scheduled")
            self.assertEqual(retry_event.status, ProcessStatus.waiting)
            self.assertEqual(retry_event.data["next_status"], "waiting")
            self.assertIn("retry_after", retry_event.data)

            await asyncio.sleep(0.03)
            second = await scheduler.claim_next(
                run_id="run_retry_failure_delay",
                document_ids=["doc_retry_failure_delay"],
                worker_id="worker-2",
            )
            self.assertIsNotNone(second)
            assert second is not None
            self.assertEqual(second.attempt, 2)

        asyncio.run(exercise())

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

    def test_runtime_service_tracks_first_class_run_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            pipeline = PipelineSpec(
                id="run_lifecycle",
                steps=[
                    ProcessSpec(
                        id="extract",
                        adapter=AdapterSpec(kind="queue", queue="lifecycle.extract"),
                    )
                ],
            )
            store = SQLiteStateStore(Path(tmp) / "runtime.db")
            service = RuntimeService(
                registry=PipelineRegistry([pipeline]),
                store=store,
            )

            run = asyncio.run(
                service.create_run(
                    run_id="run_lifecycle_1",
                    title="Lifecycle smoke",
                    metadata={"source_type": "pdf"},
                )
            )
            self.assertEqual(run.status, RunStatus.created)

            asyncio.run(
                service.initialize_document(
                    run_id="run_lifecycle_1",
                    document_id="doc.pdf",
                    pipeline_id="run_lifecycle",
                    values={"source": "doc.pdf"},
                )
            )
            queued = asyncio.run(service.get_run("run_lifecycle_1"))
            self.assertIsNotNone(queued)
            assert queued is not None
            self.assertEqual(queued.status, RunStatus.queued)
            self.assertEqual(queued.summary["document_count"], 1)

            claim = asyncio.run(
                service.claim_next(
                    run_id="run_lifecycle_1",
                    pipeline_id="run_lifecycle",
                    worker_id="worker-1",
                    adapter_kind="queue",
                )
            )
            self.assertIsNotNone(claim)
            running = asyncio.run(service.get_run("run_lifecycle_1"))
            self.assertIsNotNone(running)
            assert running is not None
            self.assertEqual(running.status, RunStatus.running)
            self.assertIsNotNone(running.started_at)

            asyncio.run(
                store.put_output(
                    run_id="run_lifecycle_1",
                    document_id="doc.pdf",
                    process_id="extract",
                    output=ProcessOutput(values={"ok": True}),
                )
            )
            completed = asyncio.run(service.sync_run_lifecycle("run_lifecycle_1"))
            self.assertEqual(completed.status, RunStatus.completed)
            self.assertEqual(completed.outcome.value, "success")
            self.assertIsNotNone(completed.finished_at)

            reopened = SQLiteStateStore(Path(tmp) / "runtime.db")
            persisted = asyncio.run(reopened.get_run("run_lifecycle_1"))
            self.assertIsNotNone(persisted)
            assert persisted is not None
            self.assertEqual(persisted.status, RunStatus.completed)
            rows = asyncio.run(reopened.list_runs())
            self.assertEqual(rows[0]["run"]["title"], "Lifecycle smoke")

    def test_runtime_run_retention_plans_and_deletes_old_terminal_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "runtime.db"
            pipeline = PipelineSpec(
                id="retention_flow",
                steps=[
                    ProcessSpec(
                        id="extract",
                        adapter=AdapterSpec(kind="queue", queue="retention.extract"),
                    )
                ],
            )
            store = SQLiteStateStore(db_path)
            service = RuntimeService(
                registry=PipelineRegistry([pipeline]),
                store=store,
            )

            async def seed() -> None:
                for run_id, document_id in [
                    ("old_done", "old_done.pdf"),
                    ("old_running", "old_running.pdf"),
                    ("new_done", "new_done.pdf"),
                ]:
                    await service.create_run_with_documents(
                        RuntimeRunInput(
                            run_id=run_id,
                            pipeline_id="retention_flow",
                            documents=[RuntimeDocumentInput(document_id=document_id)],
                        )
                    )
                for run_id, document_id in [
                    ("old_done", "old_done.pdf"),
                    ("new_done", "new_done.pdf"),
                ]:
                    await service.put_process_output(
                        run_id=run_id,
                        document_id=document_id,
                        process_id="extract",
                        output=ProcessOutput(values={"ok": True}),
                        pipeline_id="retention_flow",
                    )
                    await store.set_status(
                        run_id=run_id,
                        document_id=document_id,
                        process_id="extract",
                        status=ProcessStatus.completed,
                    )
                    await service.sync_run_lifecycle(run_id)
                claim = await service.claim_next(
                    run_id="old_running",
                    pipeline_id="retention_flow",
                    worker_id="worker-retention",
                    adapter_kind="queue",
                )
                self.assertIsNotNone(claim)

            asyncio.run(seed())

            old_ts = "2000-01-01 00:00:00+00:00"
            conn = sqlite3.connect(db_path)
            try:
                for run_id in ["old_done", "old_running"]:
                    conn.execute(
                        """
                        UPDATE process_runs
                        SET created_at = ?,
                            updated_at = ?,
                            finished_at = COALESCE(finished_at, ?)
                        WHERE id = ?
                        """,
                        (old_ts, old_ts, old_ts, run_id),
                    )
                    conn.execute(
                        "UPDATE process_documents SET updated_at = ? WHERE run_id = ?",
                        (old_ts, run_id),
                    )
                    conn.execute(
                        "UPDATE process_events SET ts = ? WHERE run_id = ?",
                        (old_ts, run_id),
                    )
                    conn.execute(
                        "UPDATE process_statuses SET updated_at = ? WHERE run_id = ?",
                        (old_ts, run_id),
                    )
                    conn.execute(
                        """
                        UPDATE process_document_inputs
                        SET updated_at = ?
                        WHERE run_id = ?
                        """,
                        (old_ts, run_id),
                    )
                    conn.execute(
                        """
                        UPDATE process_claims
                        SET claimed_at = ?, expires_at = ?
                        WHERE run_id = ?
                        """,
                        (old_ts, old_ts, run_id),
                    )
                    conn.execute(
                        "UPDATE process_outputs SET updated_at = ? WHERE run_id = ?",
                        (old_ts, run_id),
                    )
                    conn.execute(
                        """
                        UPDATE process_worker_heartbeats
                        SET last_seen_at = ?
                        WHERE run_id = ?
                        """,
                        (old_ts, run_id),
                    )
                    conn.execute(
                        """
                        UPDATE process_stream_chunks
                        SET created_at = ?
                        WHERE run_id = ?
                        """,
                        (old_ts, run_id),
                    )
                    conn.execute(
                        """
                        UPDATE process_stream_checkpoints
                        SET updated_at = ?
                        WHERE run_id = ?
                        """,
                        (old_ts, run_id),
                    )
                    conn.execute(
                        """
                        UPDATE process_projections
                        SET updated_at = ?
                        WHERE run_id = ?
                        """,
                        (old_ts, run_id),
                    )
                conn.commit()
            finally:
                conn.close()

            async def exercise() -> None:
                cutoff = datetime.now(timezone.utc) - timedelta(days=1)
                dry_run = await service.run_retention(before=cutoff)
                self.assertTrue(dry_run.dry_run)
                self.assertEqual(dry_run.candidate_count, 1)
                self.assertEqual(dry_run.runs[0].run_id, "old_done")
                self.assertEqual(dry_run.runs[0].status, RunStatus.completed)
                self.assertIsNotNone(await store.get_run("old_done"))

                app = FastAPI()
                app.include_router(create_runtime_router(service), prefix="/api")
                async with ProcessRuntimeClient(
                    "http://runtime.test",
                    transport=httpx.ASGITransport(app=app),
                ) as client:
                    client_plan = await client.run_retention(older_than_days=1)
                    self.assertTrue(client_plan["dry_run"])
                    self.assertEqual(client_plan["candidate_count"], 1)

                    deleted = await client.run_retention(
                        older_than_days=1,
                        delete=True,
                    )
                    self.assertFalse(deleted["dry_run"])
                    self.assertEqual(deleted["deleted_run_count"], 1)
                    self.assertEqual(deleted["runs"][0]["run_id"], "old_done")
                    self.assertGreater(deleted["row_counts"]["process_runs"], 0)

                self.assertIsNone(await store.get_run("old_done"))
                self.assertEqual(await store.list_documents(run_id="old_done"), [])
                self.assertIsNotNone(await store.get_run("old_running"))
                self.assertIsNotNone(await store.get_run("new_done"))

            asyncio.run(exercise())

    def test_operator_audit_records_api_actions_and_persists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "runtime.db"
            pipeline = PipelineSpec(
                id="audit_flow",
                steps=[
                    ProcessSpec(
                        id="extract",
                        adapter=AdapterSpec(kind="queue", queue="audit.extract"),
                    )
                ],
            )
            registry = PipelineRegistry([pipeline])
            store = SQLiteStateStore(db_path)
            service = RuntimeService(registry=registry, store=store)
            app = FastAPI()
            app.include_router(create_runtime_router(service), prefix="/api")

            async def exercise() -> None:
                raw_client = httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://runtime.test",
                    headers={
                        "x-fala-actor": "operator@example.com",
                        "x-fala-source": "test-suite",
                    },
                )
                async with raw_client:
                    client = ProcessRuntimeClient(
                        "http://runtime.test",
                        client=raw_client,
                    )
                    await client.create_run(
                        run_id="audit_run",
                        pipeline_id="audit_flow",
                        documents=[RuntimeDocumentInput(document_id="doc.txt")],
                    )
                    await client.control_run(
                        run_id="audit_run",
                        action="pause",
                        reason="inspect",
                    )
                    page = await client.operator_audit(run_id="audit_run")

                actions = [event.action for event in page.events]
                self.assertEqual(actions[:2], ["run.pause", "run.create"])
                pause = page.events[0]
                self.assertEqual(pause.actor, "operator@example.com")
                self.assertEqual(pause.source, "test-suite")
                self.assertEqual(pause.run_id, "audit_run")
                self.assertEqual(pause.target, "run:audit_run")
                self.assertEqual(pause.data["reason"], "inspect")

                reopened = RuntimeService(
                    registry=registry,
                    store=SQLiteStateStore(db_path),
                )
                persisted = await reopened.operator_audit(
                    run_id="audit_run",
                    descending=False,
                )
                self.assertEqual(
                    [event.action for event in persisted.events],
                    ["run.create", "run.pause"],
                )
                self.assertEqual(
                    persisted.events[0].actor,
                    "operator@example.com",
                )

            asyncio.run(exercise())

    def test_runtime_api_enforces_api_key_roles(self) -> None:
        pipeline = PipelineSpec(
            id="auth_flow",
            steps=[
                ProcessSpec(
                    id="extract",
                    adapter=AdapterSpec(kind="queue", queue="auth.extract"),
                )
            ],
        )
        service = RuntimeService(
            registry=PipelineRegistry([pipeline]),
            store=InMemoryStateStore(),
        )
        policy = RuntimeAccessPolicy.from_key_specs(
            {
                "viewer-key": {"role": "viewer", "actor": "viewer@example.com"},
                "worker-key": {"role": "worker", "actor": "worker@example.com"},
                "operator-key": {"role": "operator", "actor": "operator@example.com"},
                "admin-key": {"role": "admin", "actor": "admin@example.com"},
            }
        )
        app = FastAPI()
        app.include_router(
            create_runtime_router(service, access_policy=policy),
            prefix="/api",
        )

        async def exercise() -> None:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport,
                base_url="http://runtime.test",
            ) as raw:
                missing = await raw.get("/api/process-runtime/pipelines")
                self.assertEqual(missing.status_code, 401)

                viewer_get = await raw.get(
                    "/api/process-runtime/pipelines",
                    headers={"authorization": "Bearer viewer-key"},
                )
                self.assertEqual(viewer_get.status_code, 200)

                viewer_validate = await raw.post(
                    "/api/process-runtime/runs/validate",
                    headers={"authorization": "Bearer viewer-key"},
                    json={
                        "run_id": "viewer_validate",
                        "pipeline_id": "auth_flow",
                        "documents": [{"document_id": "doc.txt"}],
                    },
                )
                self.assertEqual(viewer_validate.status_code, 200)

                viewer_post = await raw.post(
                    "/api/process-runtime/runs",
                    headers={"authorization": "Bearer viewer-key"},
                    json={
                        "run_id": "viewer_run",
                        "pipeline_id": "auth_flow",
                        "documents": [{"document_id": "doc.txt"}],
                    },
                )
                self.assertEqual(viewer_post.status_code, 403)

                worker_create = await raw.post(
                    "/api/process-runtime/runs",
                    headers={"authorization": "Bearer worker-key"},
                    json={
                        "run_id": "worker_run",
                        "pipeline_id": "auth_flow",
                        "documents": [{"document_id": "doc.txt"}],
                    },
                )
                self.assertEqual(worker_create.status_code, 403)

                admin_delete = await raw.post(
                    "/api/process-runtime/artifacts/gc",
                    headers={"authorization": "Bearer operator-key"},
                    json={"delete": True},
                )
                self.assertEqual(admin_delete.status_code, 403)

            async with ProcessRuntimeClient(
                "http://runtime.test",
                transport=httpx.ASGITransport(app=app),
                api_key="operator-key",
            ) as client:
                created = await client.create_run(
                    run_id="auth_run",
                    pipeline_id="auth_flow",
                    documents=[RuntimeDocumentInput(document_id="doc.txt")],
                )
                self.assertTrue(created["ok"])
                audit = await client.operator_audit(run_id="auth_run")
                self.assertEqual(audit.events[0].actor, "operator@example.com")

            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://runtime.test",
            ) as raw:
                worker_heartbeat = await raw.post(
                    "/api/runs/auth_run/process-runtime/workers/worker-1/heartbeat",
                    headers={"authorization": "Bearer worker-key"},
                    json={"pipeline_id": "auth_flow", "status": "idle"},
                )
                self.assertEqual(worker_heartbeat.status_code, 200)

                viewer_heartbeat = await raw.post(
                    "/api/runs/auth_run/process-runtime/workers/worker-1/heartbeat",
                    headers={"authorization": "Bearer viewer-key"},
                    json={"pipeline_id": "auth_flow", "status": "idle"},
                )
                self.assertEqual(viewer_heartbeat.status_code, 403)

        asyncio.run(exercise())

    def test_runtime_web_enforces_api_key_roles(self) -> None:
        pipeline = PipelineSpec(
            id="web_auth_flow",
            steps=[
                ProcessSpec(
                    id="extract",
                    adapter=AdapterSpec(kind="queue", queue="web.extract"),
                )
            ],
        )
        service = RuntimeService(
            registry=PipelineRegistry([pipeline]),
            store=InMemoryStateStore(),
        )
        policy = RuntimeAccessPolicy.from_key_specs(
            {
                "viewer-key": {"role": "viewer", "actor": "viewer@example.com"},
                "operator-key": {"role": "operator", "actor": "operator@example.com"},
            }
        )

        async def seed() -> None:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="web_auth_run",
                    pipeline_id="web_auth_flow",
                    documents=[RuntimeDocumentInput(document_id="doc.txt")],
                )
            )

        asyncio.run(seed())

        app = create_runtime_web_app(service=service, access_policy=policy)

        async def exercise() -> None:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://runtime.test",
                follow_redirects=False,
            ) as raw:
                missing = await raw.get("/runs")
                self.assertEqual(missing.status_code, 401)

                viewer_get = await raw.get(
                    "/runs",
                    headers={"authorization": "Bearer viewer-key"},
                )
                self.assertEqual(viewer_get.status_code, 200)

                viewer_post = await raw.post(
                    "/runs/web_auth_run/actions/pause",
                    headers={"authorization": "Bearer viewer-key"},
                )
                self.assertEqual(viewer_post.status_code, 403)

                operator_post = await raw.post(
                    "/runs/web_auth_run/actions/pause",
                    headers={"authorization": "Bearer operator-key"},
                )
                self.assertEqual(operator_post.status_code, 303)

        asyncio.run(exercise())

    def test_runtime_api_scopes_runs_by_api_key_tenant(self) -> None:
        pipeline = PipelineSpec(
            id="tenant_flow",
            steps=[
                ProcessSpec(
                    id="extract",
                    adapter=AdapterSpec(kind="queue", queue="tenant.extract"),
                )
            ],
        )
        service = RuntimeService(
            registry=PipelineRegistry([pipeline]),
            store=InMemoryStateStore(),
        )
        policy = RuntimeAccessPolicy.from_key_specs(
            {
                "tenant-a-key": {
                    "role": "operator",
                    "actor": "a@example.com",
                    "tenant_id": "tenant-a",
                },
                "tenant-b-key": {
                    "role": "operator",
                    "actor": "b@example.com",
                    "tenant_id": "tenant-b",
                },
                "admin-key": {"role": "admin", "actor": "admin@example.com"},
            }
        )
        app = FastAPI()
        app.include_router(
            create_runtime_router(service, access_policy=policy),
            prefix="/api",
        )

        async def exercise() -> None:
            async with ProcessRuntimeClient(
                "http://runtime.test",
                transport=httpx.ASGITransport(app=app),
                api_key="tenant-a-key",
            ) as client:
                await client.create_run(
                    run_id="tenant_run",
                    pipeline_id="tenant_flow",
                    documents=[RuntimeDocumentInput(document_id="doc.txt")],
                )

            stored = await service.get_run("tenant_run")
            self.assertIsNotNone(stored)
            assert stored is not None
            self.assertEqual(stored.metadata["tenant_id"], "tenant-a")

            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://runtime.test",
            ) as raw:
                tenant_a_list = await raw.get(
                    "/api/process-runtime/runs",
                    headers={"authorization": "Bearer tenant-a-key"},
                )
                self.assertEqual(tenant_a_list.status_code, 200)
                self.assertEqual(
                    [item["run_id"] for item in tenant_a_list.json()["runs"]],
                    ["tenant_run"],
                )

                tenant_b_list = await raw.get(
                    "/api/process-runtime/runs",
                    headers={"authorization": "Bearer tenant-b-key"},
                )
                self.assertEqual(tenant_b_list.status_code, 200)
                self.assertEqual(tenant_b_list.json()["runs"], [])

                tenant_b_get = await raw.get(
                    "/api/process-runtime/runs/tenant_run",
                    headers={"authorization": "Bearer tenant-b-key"},
                )
                self.assertEqual(tenant_b_get.status_code, 404)

                tenant_b_append = await raw.post(
                    "/api/runs/tenant_run/process-runtime/documents/batch",
                    headers={"authorization": "Bearer tenant-b-key"},
                    json={
                        "pipeline_id": "tenant_flow",
                        "documents": [{"document_id": "other.txt"}],
                    },
                )
                self.assertEqual(tenant_b_append.status_code, 404)

                admin_list = await raw.get(
                    "/api/process-runtime/runs",
                    headers={"authorization": "Bearer admin-key"},
                )
                self.assertEqual(admin_list.status_code, 200)
                self.assertEqual(
                    [item["run_id"] for item in admin_list.json()["runs"]],
                    ["tenant_run"],
                )

        asyncio.run(exercise())

    def test_runtime_create_run_with_documents_records_provenance_snapshot(self) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="pkg",
                pipelines=["case.yaml"],
                document_types=[DocumentTypeSpec(id="case_document")],
                artifact_kinds=[
                    ArtifactKindSpec(id="source_payload"),
                    ArtifactKindSpec(id="classified_payload"),
                ],
                capabilities=[
                    CapabilitySpec(
                        id="ingest_case",
                        accepts_document_types=["case_document"],
                        emits_artifact_kinds=["source_payload"],
                    ),
                    CapabilitySpec(
                        id="classify_case",
                        accepts_artifact_kinds=["source_payload"],
                        emits_artifact_kinds=["classified_payload"],
                    ),
                ],
                workers=[
                    WorkflowWorkerSpec(
                        id="ingest_worker",
                        capabilities=["ingest_case"],
                        pipeline_id="case_flow",
                        process_id="ingest",
                        command=["python", "workers/ingest.py"],
                    )
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="case_flow",
                steps=[
                    ProcessSpec(
                        id="ingest",
                        capability="ingest_case",
                        adapter=AdapterSpec(kind="queue", queue="case.ingest"),
                    ),
                    ProcessSpec(
                        id="classify",
                        capability="classify_case",
                        needs=["ingest"],
                        adapter=AdapterSpec(kind="queue", queue="case.classify"),
                    ),
                ],
            ),
            package_id="pkg",
        )
        registry.validate_package_workers("pkg")
        service = RuntimeService(registry=registry, store=InMemoryStateStore())

        async def run_api() -> None:
            run, _schedules = await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_provenance",
                    pipeline_id="case_flow",
                    metadata={"source": "fixture"},
                    documents=[
                        RuntimeDocumentInput(
                            document_id="case-a",
                            document_type="case_document",
                            source_uri="file:///tmp/case-a.pdf",
                        ),
                        RuntimeDocumentInput(
                            document_id="case-b",
                            document_type="case_document",
                            source_uri="file:///tmp/case-b.pdf",
                        ),
                    ],
                )
            )
            provenance = run.metadata["process_runtime"]["run_provenance"]
            self.assertEqual(run.metadata["source"], "fixture")
            self.assertEqual(provenance["schema_version"], 1)
            self.assertEqual(len(provenance["run_input_sha256"]), 64)
            self.assertEqual(len(provenance["pipeline_contracts_sha256"]), 64)
            self.assertEqual(len(provenance["plan_sha256"]), 64)
            self.assertEqual(provenance["document_count"], 2)
            self.assertEqual(provenance["pipeline_ids"], ["case_flow"])
            self.assertNotIn("route_report", provenance)
            self.assertNotIn("route_report_sha256", provenance)
            self.assertEqual(
                provenance["document_summary"]["pipeline_counts"],
                {"case_flow": 2},
            )
            self.assertEqual(provenance["plan"]["process_instance_count"], 4)
            self.assertEqual(provenance["plan"]["queued_count"], 2)
            self.assertEqual(provenance["plan"]["waiting_count"], 2)
            self.assertNotIn("documents", provenance["plan"])
            ingest = next(
                item
                for item in provenance["plan"]["processes"]
                if item["process_id"] == "ingest"
            )
            self.assertEqual(ingest["declared_worker_ids"], ["ingest_worker"])
            self.assertEqual(
                provenance["pipeline_contracts"]["case_flow"]["pipeline_id"],
                "case_flow",
            )
            persisted = await service.get_run("run_provenance")
            self.assertIsNotNone(persisted)
            assert persisted is not None
            self.assertEqual(
                persisted.metadata["process_runtime"]["run_provenance"]["plan_sha256"],
                provenance["plan_sha256"],
            )
            page = await service.run_provenance("run_provenance")
            self.assertTrue(page["contract_drift"]["ok"])
            self.assertFalse(page["contract_drift"]["drifted"])
            self.assertEqual(
                page["contract_drift"]["pipelines"][0]["status"],
                "unchanged",
            )

        asyncio.run(run_api())

    def test_runtime_provenance_reports_pipeline_contract_drift(self) -> None:
        def registry_for_step_title(title: str) -> PipelineRegistry:
            registry = PipelineRegistry()
            registry.add_package(
                WorkflowPackageSpec(
                    id="pkg",
                    pipelines=["case.yaml"],
                    document_types=[DocumentTypeSpec(id="case_document")],
                    artifact_kinds=[ArtifactKindSpec(id="source_payload")],
                    capabilities=[
                        CapabilitySpec(
                            id="ingest_case",
                            accepts_document_types=["case_document"],
                            emits_artifact_kinds=["source_payload"],
                        ),
                    ],
                )
            )
            registry.add(
                PipelineSpec(
                    id="case_flow",
                    steps=[
                        ProcessSpec(
                            id="ingest",
                            title=title,
                            capability="ingest_case",
                            adapter=AdapterSpec(kind="queue", queue="case.ingest"),
                        ),
                    ],
                ),
                package_id="pkg",
            )
            return registry

        store = InMemoryStateStore()
        service = RuntimeService(
            registry=registry_for_step_title("Ingest v1"),
            store=store,
        )

        async def run_api() -> None:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_drift",
                    pipeline_id="case_flow",
                    documents=[
                        RuntimeDocumentInput(
                            document_id="case-a",
                            document_type="case_document",
                        ),
                    ],
                )
            )
            current = await service.run_provenance("run_drift")
            self.assertTrue(current["contract_drift"]["ok"])

            drifted_service = RuntimeService(
                registry=registry_for_step_title("Ingest v2"),
                store=store,
            )
            drifted = await drifted_service.run_provenance("run_drift")
            self.assertFalse(drifted["contract_drift"]["ok"])
            self.assertTrue(drifted["contract_drift"]["drifted"])
            self.assertEqual(
                drifted["contract_drift"]["changed_pipeline_ids"],
                ["case_flow"],
            )
            self.assertEqual(
                drifted["contract_drift"]["pipelines"][0]["status"],
                "changed",
            )
            self.assertNotEqual(
                drifted["contract_drift"]["pipelines"][0]["stored_sha256"],
                drifted["contract_drift"]["pipelines"][0]["current_sha256"],
            )

            app = FastAPI()
            app.include_router(create_runtime_router(drifted_service), prefix="/api")
            async with ProcessRuntimeClient(
                "http://runtime.test",
                transport=httpx.ASGITransport(app=app),
            ) as client:
                api_page = await client.run_provenance(run_id="run_drift")
                self.assertTrue(api_page["contract_drift"]["drifted"])
                await drifted_service.pause_run("run_drift", reason="test pause")
                with self.assertRaises(httpx.HTTPStatusError) as resume_error:
                    await client.control_run(
                        run_id="run_drift",
                        action="resume",
                        reason="blocked drift",
                    )
                self.assertEqual(resume_error.exception.response.status_code, 400)
                self.assertIn(
                    "contract drift detected",
                    resume_error.exception.response.text,
                )
                resumed = await client.control_run(
                    run_id="run_drift",
                    action="resume",
                    reason="allow drift",
                    allow_contract_drift=True,
                )
                self.assertEqual(resumed["run"]["status"], "queued")
                with self.assertRaises(httpx.HTTPStatusError) as retry_error:
                    await client.control_process(
                        run_id="run_drift",
                        document_id="case-a",
                        process_id="ingest",
                        action=ProcessAction.retry,
                        reason="blocked drift",
                    )
                self.assertEqual(retry_error.exception.response.status_code, 400)
                retry = await client.control_process(
                    run_id="run_drift",
                    document_id="case-a",
                    process_id="ingest",
                    action=ProcessAction.retry,
                    reason="allow drift",
                    allow_contract_drift=True,
                )
                self.assertEqual(retry.action, ProcessAction.retry)

            web = create_runtime_web_app(service=drifted_service)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=web),
                base_url="http://testserver",
            ) as raw:
                response = await raw.get("/runs/run_drift/process-runtime/provenance")
                self.assertEqual(response.status_code, 200)
                self.assertIn("Contract drift", response.text)
                self.assertIn("changed", response.text)

        asyncio.run(run_api())

    def test_runtime_run_api_creates_and_reads_runs(self) -> None:
        service = RuntimeService(
            registry=PipelineRegistry([]),
            store=InMemoryStateStore(),
        )
        app = FastAPI()
        app.include_router(create_runtime_router(service), prefix="/api")

        async def run_api() -> None:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport,
                base_url="http://testserver",
            ) as client:
                created = await client.post(
                    "/api/process-runtime/runs",
                    json={
                        "run_id": "run_api_1",
                        "title": "API run",
                        "metadata": {"source_type": "email"},
                    },
                )
                self.assertEqual(created.status_code, 200)
                self.assertEqual(created.json()["run"]["status"], "created")

                listed = await client.get("/api/process-runtime/runs")
                self.assertEqual(listed.status_code, 200)
                self.assertEqual(listed.json()["runs"][0]["run_id"], "run_api_1")
                self.assertEqual(listed.json()["runs"][0]["title"], "API run")

                fetched = await client.get("/api/process-runtime/runs/run_api_1")
                self.assertEqual(fetched.status_code, 200)
                self.assertEqual(fetched.json()["run"]["metadata"]["source_type"], "email")

        asyncio.run(run_api())

    def test_runtime_run_api_creates_run_with_document_batch(self) -> None:
        pipeline = PipelineSpec(
            id="batch_flow",
            steps=[
                ProcessSpec(
                    id="ingest",
                    adapter=AdapterSpec(kind="queue", queue="batch.ingest"),
                )
            ],
        )
        store = InMemoryStateStore()
        service = RuntimeService(
            registry=PipelineRegistry([pipeline]),
            store=store,
        )
        app = FastAPI()
        app.include_router(create_runtime_router(service), prefix="/api")

        async def run_api() -> None:
            async with ProcessRuntimeClient(
                "http://testserver",
                transport=httpx.ASGITransport(app=app),
            ) as client:
                created = await client.create_run(
                    run_id="run_batch",
                    title="Batch import",
                    pipeline_id="batch_flow",
                    metadata={"source": "dropbox"},
                    documents=[
                        RuntimeDocumentInput(
                            document_id="doc_1.pdf",
                            title="Doc 1",
                            document_type="generic_document",
                            media_type="application/pdf",
                            source_uri="file:///tmp/doc_1.pdf",
                            values={"case_id": "A"},
                            metadata={"mailbox": "ops"},
                        ),
                        {
                            "document_id": "email_1",
                            "document_type": "generic_document",
                            "media_type": "message/rfc822",
                            "source_uri": "s3://bucket/email_1.eml",
                            "values": {"case_id": "B"},
                        },
                    ],
                )
                self.assertEqual(created["run"]["id"], "run_batch")
                self.assertEqual(created["run"]["status"], "queued")
                self.assertEqual(len(created["schedules"]), 2)
                documents = await client.list_documents(run_id="run_batch")
                self.assertEqual(len(documents), 2)
                self.assertEqual(documents[0]["document_id"], "doc_1.pdf")
                self.assertEqual(documents[0]["title"], "Doc 1")
                self.assertEqual(documents[0]["status"], RuntimeDocumentStatus.queued.value)
                resumed = await client.create_run(
                    run_id="run_batch",
                    title="Changed import",
                    pipeline_id="batch_flow",
                    existing_run_policy="resume",
                    existing_document_policy="reuse",
                    documents=[
                        {
                            "document_id": "doc_1.pdf",
                            "source_uri": "file:///tmp/changed.pdf",
                            "values": {"case_id": "changed"},
                            "metadata": {"mailbox": "changed"},
                        }
                    ],
                )
                self.assertEqual(resumed["run"]["title"], "Batch import")
                self.assertEqual(len(resumed["schedules"]), 1)

        asyncio.run(run_api())

        doc_1 = asyncio.run(
            store.get_document(run_id="run_batch", document_id="doc_1.pdf")
        )
        email = asyncio.run(
            store.get_document(run_id="run_batch", document_id="email_1")
        )
        self.assertIsNotNone(doc_1)
        self.assertIsNotNone(email)
        assert doc_1 is not None
        assert email is not None
        self.assertEqual(doc_1.title, "Doc 1")
        self.assertEqual(doc_1.document_type, "generic_document")
        self.assertEqual(doc_1.media_type, "application/pdf")
        self.assertEqual(doc_1.source_uri, "file:///tmp/doc_1.pdf")
        self.assertEqual(doc_1.metadata["mailbox"], "ops")
        self.assertEqual(doc_1.status, RuntimeDocumentStatus.queued)
        self.assertEqual(doc_1.summary["process_count"], 1)
        self.assertEqual(email.media_type, "message/rfc822")
        doc_1_input = asyncio.run(
            store.get_document_input(run_id="run_batch", document_id="doc_1.pdf")
        )
        email_input = asyncio.run(
            store.get_document_input(run_id="run_batch", document_id="email_1")
        )
        self.assertIsNotNone(doc_1_input)
        self.assertIsNotNone(email_input)
        assert doc_1_input is not None
        assert email_input is not None
        self.assertEqual(doc_1_input.values["case_id"], "A")
        self.assertEqual(doc_1_input.values["document"]["type"], "generic_document")
        self.assertEqual(doc_1_input.values["document"]["metadata"]["mailbox"], "ops")
        self.assertEqual(doc_1_input.artifacts[0].kind, "generic_document")
        self.assertEqual(doc_1_input.artifacts[0].uri, "file:///tmp/doc_1.pdf")
        self.assertEqual(email_input.values["document"]["media_type"], "message/rfc822")

    def test_runtime_batch_creation_can_resume_existing_run_and_reuse_documents(self) -> None:
        pipeline = PipelineSpec(
            id="batch_flow",
            steps=[
                ProcessSpec(
                    id="ingest",
                    adapter=AdapterSpec(kind="queue", queue="batch.ingest"),
                )
            ],
        )
        store = InMemoryStateStore()
        service = RuntimeService(registry=PipelineRegistry([pipeline]), store=store)

        async def run_api() -> None:
            run, schedules = await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_resume_batch",
                    title="First batch",
                    pipeline_id="batch_flow",
                    documents=[
                        RuntimeDocumentInput(
                            document_id="doc_1",
                            source_uri="file:///tmp/first.txt",
                            values={"case_id": "A"},
                            metadata={"origin": "first"},
                        )
                    ],
                )
            )
            self.assertEqual(run.id, "run_resume_batch")
            self.assertEqual(len(schedules), 1)

            with self.assertRaisesRegex(ValueError, "already exists"):
                await service.create_run_with_documents(
                    RuntimeRunInput(
                        run_id="run_resume_batch",
                        pipeline_id="batch_flow",
                        documents=[
                            RuntimeDocumentInput(
                                document_id="doc_1",
                                source_uri="file:///tmp/changed.txt",
                            )
                        ],
                    )
                )
            with self.assertRaisesRegex(ValueError, "already exists in"):
                await service.create_run_with_documents(
                    RuntimeRunInput(
                        run_id="run_resume_batch",
                        existing_run_policy="resume",
                        pipeline_id="batch_flow",
                        documents=[
                            RuntimeDocumentInput(
                                document_id="doc_new_before_conflict",
                                source_uri="file:///tmp/new.txt",
                            ),
                            RuntimeDocumentInput(
                                document_id="doc_1",
                                source_uri="file:///tmp/changed.txt",
                            ),
                        ],
                    )
                )
            documents = await store.list_document_records(run_id="run_resume_batch")
            self.assertEqual([document.document_id for document in documents], ["doc_1"])

            resumed, resumed_schedules = await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_resume_batch",
                    existing_run_policy="resume",
                    existing_document_policy="reuse",
                    title="Changed batch",
                    pipeline_id="batch_flow",
                    documents=[
                        RuntimeDocumentInput(
                            document_id="doc_1",
                            source_uri="file:///tmp/changed.txt",
                            values={"case_id": "B"},
                            metadata={"origin": "changed"},
                        )
                    ],
                )
            )
            self.assertEqual(resumed.id, "run_resume_batch")
            self.assertEqual(resumed.title, "First batch")
            self.assertEqual(len(resumed_schedules), 1)

            documents = await store.list_document_records(run_id="run_resume_batch")
            self.assertEqual([document.document_id for document in documents], ["doc_1"])
            doc_1 = await store.get_document(
                run_id="run_resume_batch",
                document_id="doc_1",
            )
            doc_1_input = await store.get_document_input(
                run_id="run_resume_batch",
                document_id="doc_1",
            )
            self.assertIsNotNone(doc_1)
            self.assertIsNotNone(doc_1_input)
            assert doc_1 is not None
            assert doc_1_input is not None
            self.assertEqual(doc_1.source_uri, "file:///tmp/first.txt")
            self.assertEqual(doc_1.metadata["origin"], "first")
            self.assertEqual(doc_1_input.values["case_id"], "A")
            self.assertEqual(doc_1_input.artifacts[0].uri, "file:///tmp/first.txt")

            added, added_schedules = await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_resume_batch",
                    existing_run_policy="resume",
                    pipeline_id="batch_flow",
                    documents=[
                        RuntimeDocumentInput(
                            document_id="doc_2",
                            source_uri="file:///tmp/second.txt",
                        )
                    ],
                )
            )
            self.assertEqual(added.id, "run_resume_batch")
            self.assertEqual(len(added_schedules), 1)
            documents = await store.list_document_records(run_id="run_resume_batch")
            self.assertEqual(
                [document.document_id for document in documents],
                ["doc_1", "doc_2"],
            )

        asyncio.run(run_api())

    def test_runtime_can_append_documents_to_existing_run_from_api_and_cli(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "runtime.db"
            registry = PipelineRegistry.from_directory(
                Path(__file__).resolve().parents[1]
                / "examples"
                / "pipelines"
            )
            service = RuntimeService(
                registry=registry,
                store=SQLiteStateStore(db_path),
            )

            async def seed() -> None:
                await service.create_run_with_documents(
                    RuntimeRunInput(
                        run_id="run_append",
                        pipeline_id="basic_enrichment",
                        documents=[
                            RuntimeDocumentInput(
                                document_id="doc_1",
                                values={"source": "one.txt"},
                            )
                        ],
                    )
                )

            asyncio.run(seed())

            app = FastAPI()
            app.include_router(create_runtime_router(service), prefix="/api")

            async def exercise_api() -> None:
                raw_client = httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://runtime.test",
                    headers={
                        "x-fala-actor": "importer@example.com",
                        "x-fala-source": "batch-importer",
                    },
                )
                async with raw_client:
                    client = ProcessRuntimeClient(
                        "http://runtime.test",
                        client=raw_client,
                    )
                    appended = await client.append_documents(
                        run_id="run_append",
                        pipeline_id="basic_enrichment",
                        documents=[
                            RuntimeDocumentInput(
                                document_id="doc_2",
                                values={"source": "two.txt"},
                            ),
                            {
                                "document_id": "doc_3",
                                "values": {"source": "three.txt"},
                            },
                        ],
                    )
                    self.assertTrue(appended["ok"])
                    self.assertEqual(appended["document_count"], 2)
                    self.assertEqual(
                        [item["document_id"] for item in appended["schedules"]],
                        ["doc_2", "doc_3"],
                    )
                    documents = await client.list_documents(run_id="run_append")
                    self.assertEqual(
                        [document["document_id"] for document in documents],
                        ["doc_1", "doc_2", "doc_3"],
                    )
                    page = await client.document_page(
                        run_id="run_append",
                        status=RuntimeDocumentStatus.queued.value,
                        pipeline_id="basic_enrichment",
                        limit=2,
                    )
                    self.assertEqual(page.count, 2)
                    self.assertTrue(page.has_more)
                    self.assertEqual(page.filters["status"], "queued")
                    self.assertEqual(page.filters["pipeline_id"], "basic_enrichment")
                    self.assertEqual(
                        [document.document_id for document in page.documents],
                        ["doc_1", "doc_2"],
                    )
                    tail = await client.list_documents(
                        run_id="run_append",
                        status=RuntimeDocumentStatus.queued.value,
                        limit=2,
                        offset=2,
                    )
                    self.assertEqual(
                        [document["document_id"] for document in tail],
                        ["doc_3"],
                    )
                    process_page = await client.process_page(
                        run_id="run_append",
                        status=ProcessStatus.queued.value,
                        pipeline_id="basic_enrichment",
                        process_id="ingest",
                        capability="ingest_document",
                        operation_type="ingest",
                        adapter_kind="subprocess",
                        resource_pool="default",
                        limit=2,
                    )
                    self.assertEqual(process_page.count, 2)
                    self.assertTrue(process_page.has_more)
                    self.assertEqual(process_page.filters["process_id"], "ingest")
                    self.assertEqual(process_page.filters["capability"], "ingest_document")
                    self.assertEqual(process_page.filters["operation_type"], "ingest")
                    self.assertEqual(process_page.filters["adapter_kind"], "subprocess")
                    self.assertEqual(process_page.filters["resource_pool"], "default")
                    self.assertEqual(
                        [
                            (
                                process.document_id,
                                process.process_id,
                                process.capability,
                                process.operation_type,
                                process.adapter_kind,
                                process.resource_pool,
                                process.status,
                            )
                            for process in process_page.processes
                        ],
                        [
                            (
                                "doc_1",
                                "ingest",
                                "ingest_document",
                                "ingest",
                                "subprocess",
                                "default",
                                ProcessStatus.queued,
                            ),
                            (
                                "doc_2",
                                "ingest",
                                "ingest_document",
                                "ingest",
                                "subprocess",
                                "default",
                                ProcessStatus.queued,
                            ),
                        ],
                    )
                    waiting_processes = await client.list_processes(
                        run_id="run_append",
                        status=ProcessStatus.waiting.value,
                        capability="enrich_document",
                        operation_type="enrich",
                        limit=10,
                    )
                    self.assertEqual(
                        [
                            (
                                process["document_id"],
                                process["process_id"],
                                process["operation_type"],
                            )
                            for process in waiting_processes
                        ],
                        [
                            ("doc_1", "enrich", "enrich"),
                            ("doc_2", "enrich", "enrich"),
                            ("doc_3", "enrich", "enrich"),
                        ],
                    )
                    state = await client.get_state(run_id="run_append")
                    self.assertEqual(
                        state.summary.operation_type_counts,
                        {"enrich": 3, "export": 3, "ingest": 3},
                    )
                    audit = await client.operator_audit(run_id="run_append")
                    append_event = next(
                        event
                        for event in audit.events
                        if event.action == "documents.append"
                    )
                    self.assertEqual(append_event.actor, "importer@example.com")
                    self.assertEqual(append_event.source, "batch-importer")
                    self.assertEqual(
                        append_event.data["document_ids"],
                        ["doc_2", "doc_3"],
                    )
                    provenance = await client.run_provenance(run_id="run_append")
                    append_batches = provenance["provenance"]["append_batches"]
                    self.assertEqual(len(append_batches), 1)
                    self.assertEqual(append_batches[0]["batch_id"], "append-0001")
                    self.assertEqual(
                        append_batches[0]["document_ids"],
                        ["doc_2", "doc_3"],
                    )
                    self.assertEqual(append_batches[0]["scheduled_count"], 2)
                    self.assertEqual(len(append_batches[0]["append_input_sha256"]), 64)
                    self.assertNotIn("route_report", append_batches[0])

            asyncio.run(exercise_api())

            appended_cli = _run_cli(
                "--pipeline-dir",
                "examples/pipelines",
                "append-documents",
                "--db",
                str(db_path),
                "--run-id",
                "run_append",
                "--pipeline",
                "basic_enrichment",
                "--document",
                "doc_4=file:///tmp/four.txt",
            )
            self.assertTrue(appended_cli["ok"])
            self.assertEqual(appended_cli["document_count"], 1)
            self.assertEqual(
                appended_cli["schedules"][0]["document_id"],
                "doc_4",
            )
            listed_cli = _run_cli(
                "--pipeline-dir",
                "examples/pipelines",
                "list-documents",
                "--db",
                str(db_path),
                "--run-id",
                "run_append",
                "--status",
                RuntimeDocumentStatus.queued.value,
                "--limit",
                "2",
                "--offset",
                "1",
            )
            self.assertTrue(listed_cli["ok"])
            self.assertEqual(listed_cli["documents"]["count"], 2)
            self.assertTrue(listed_cli["documents"]["has_more"])
            self.assertEqual(
                [
                    document["document_id"]
                    for document in listed_cli["documents"]["documents"]
                ],
                ["doc_2", "doc_3"],
            )
            listed_processes_cli = _run_cli(
                "--pipeline-dir",
                "examples/pipelines",
                "list-processes",
                "--db",
                str(db_path),
                "--run-id",
                "run_append",
                "--status",
                ProcessStatus.queued.value,
                "--process-id",
                "ingest",
                "--capability",
                "ingest_document",
                "--operation-type",
                "ingest",
                "--adapter-kind",
                "subprocess",
                "--resource-pool",
                "default",
                "--limit",
                "2",
                "--offset",
                "1",
            )
            self.assertTrue(listed_processes_cli["ok"])
            self.assertEqual(listed_processes_cli["processes"]["count"], 2)
            self.assertTrue(listed_processes_cli["processes"]["has_more"])
            self.assertEqual(
                [
                    (process["document_id"], process["operation_type"])
                    for process in listed_processes_cli["processes"]["processes"]
                ],
                [("doc_2", "ingest"), ("doc_3", "ingest")],
            )

            async def inspect_store() -> None:
                reopened = SQLiteStateStore(db_path)
                documents = await reopened.list_document_records(run_id="run_append")
                self.assertEqual(
                    [document.document_id for document in documents],
                    ["doc_1", "doc_2", "doc_3", "doc_4"],
                )
                audit = await reopened.list_audit_events(run_id="run_append")
                self.assertIn("documents.append", [event.action for event in audit])
                run = await reopened.get_run("run_append")
                self.assertIsNotNone(run)
                assert run is not None
                append_batches = run.metadata["process_runtime"]["run_provenance"][
                    "append_batches"
                ]
                self.assertEqual(len(append_batches), 2)
                self.assertEqual(
                    [batch["batch_id"] for batch in append_batches],
                    ["append-0001", "append-0002"],
                )
                self.assertEqual(append_batches[1]["document_ids"], ["doc_4"])
                self.assertEqual(append_batches[1]["scheduled_count"], 1)

            asyncio.run(inspect_store())

            async def fetch_web_provenance() -> None:
                app = create_runtime_web_app(
                    service=RuntimeService(
                        registry=registry,
                        store=SQLiteStateStore(db_path),
                    )
                )
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://testserver",
                ) as client:
                    provenance = await client.get(
                        "/runs/run_append/process-runtime/provenance"
                    )
                    self.assertEqual(provenance.status_code, 200)
                    self.assertIn("Append batches", provenance.text)
                    self.assertIn("append-0002", provenance.text)
                    self.assertIn("doc_4", provenance.text)

            asyncio.run(fetch_web_provenance())

    def test_runtime_dead_letter_queue_lists_and_replays_failed_processes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "runtime.db"
            registry = PipelineRegistry.from_directory(
                Path(__file__).resolve().parents[1]
                / "examples"
                / "pipelines"
            )
            service = RuntimeService(
                registry=registry,
                store=SQLiteStateStore(db_path),
            )

            async def seed() -> None:
                await service.create_run_with_documents(
                    RuntimeRunInput(
                        run_id="run_dead_letter",
                        pipeline_id="basic_enrichment",
                        documents=[
                            RuntimeDocumentInput(
                                document_id="doc_failed",
                                values={"source": "bad.txt"},
                            )
                        ],
                    )
                )

            asyncio.run(seed())

            app = FastAPI()
            app.include_router(create_runtime_router(service), prefix="/api")

            async def exercise_api() -> None:
                raw_client = httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://runtime.test",
                    headers={
                        "x-fala-actor": "operator@example.com",
                        "x-fala-source": "ops-console",
                    },
                )
                async with raw_client:
                    client = ProcessRuntimeClient(
                        "http://runtime.test",
                        client=raw_client,
                    )
                    failed = await client.write_status(
                        run_id="run_dead_letter",
                        document_id="doc_failed",
                        process_id="ingest",
                        status=ProcessStatus.failed,
                        data={
                            "reason": "bad input",
                            "error_kind": "validation_error",
                        },
                    )
                    self.assertTrue(failed["ok"])
                    self.assertEqual(failed["action"]["action"], "fail")

                    page = await client.dead_letter_page(run_id="run_dead_letter")
                    self.assertEqual(page.count, 1)
                    self.assertFalse(page.has_more)
                    self.assertEqual(page.filters["status"], "failed")
                    item = page.items[0]
                    self.assertEqual(item.document_id, "doc_failed")
                    self.assertEqual(item.process_id, "ingest")
                    self.assertEqual(item.pipeline_id, "basic_enrichment")
                    self.assertEqual(item.capability, "ingest_document")
                    self.assertEqual(item.operation_type, "ingest")
                    self.assertEqual(item.adapter_kind, "subprocess")
                    self.assertEqual(item.reason, "bad input")
                    self.assertEqual(item.error_kind, "validation_error")
                    self.assertEqual(item.terminal_reason, "max_attempts_exhausted")
                    self.assertEqual(item.last_event_type, "process.failed")
                    self.assertEqual(
                        item.suggested_actions,
                        [
                            ProcessAction.retry,
                            ProcessAction.skip,
                            ProcessAction.cancel,
                        ],
                    )

                    listed = await client.list_dead_letters(
                        run_id="run_dead_letter",
                        capability="ingest_document",
                        operation_type="ingest",
                    )
                    self.assertEqual(len(listed), 1)
                    self.assertEqual(listed[0]["operation_type"], "ingest")
                    self.assertEqual(listed[0]["event_data"]["reason"], "bad input")

                    replay = await client.replay_dead_letter(
                        run_id="run_dead_letter",
                        document_id="doc_failed",
                        process_id="ingest",
                        reason="fixed source",
                    )
                    self.assertEqual(replay.action, ProcessAction.retry)
                    self.assertEqual(replay.affected, ["ingest", "enrich", "export"])

                    empty = await client.dead_letter_page(run_id="run_dead_letter")
                    self.assertEqual(empty.count, 0)

                    audit = await client.operator_audit(run_id="run_dead_letter")
                    replay_event = next(
                        event
                        for event in audit.events
                        if event.action == "process.dead_letter.replay"
                    )
                    self.assertEqual(replay_event.actor, "operator@example.com")
                    self.assertEqual(replay_event.source, "ops-console")
                    self.assertEqual(replay_event.data["reason"], "fixed source")

            asyncio.run(exercise_api())

            async def fail_again() -> None:
                await PipelineScheduler(
                    registry.get("basic_enrichment"),
                    SQLiteStateStore(db_path),
                ).record_process_failure(
                    run_id="run_dead_letter",
                    document_id="doc_failed",
                    process_id="ingest",
                    reason="still bad",
                    error_kind="validation_error",
                )

            asyncio.run(fail_again())

            listed_cli = _run_cli(
                "--pipeline-dir",
                "examples/pipelines",
                "dead-letter",
                "--db",
                str(db_path),
                "--run-id",
                "run_dead_letter",
                "--capability",
                "ingest_document",
                "--operation-type",
                "ingest",
            )
            self.assertTrue(listed_cli["ok"])
            self.assertEqual(listed_cli["dead_letter"]["count"], 1)
            self.assertEqual(
                listed_cli["dead_letter"]["items"][0]["reason"],
                "still bad",
            )

            replayed_cli = _run_cli(
                "--pipeline-dir",
                "examples/pipelines",
                "replay-dead-letter",
                "--db",
                str(db_path),
                "--run-id",
                "run_dead_letter",
                "--document-id",
                "doc_failed",
                "--process-id",
                "ingest",
                "--reason",
                "cli replay",
            )
            self.assertTrue(replayed_cli["ok"])
            self.assertEqual(replayed_cli["action"]["action"], "retry")

    def test_runtime_stuck_work_lists_sla_breaches_and_expired_claims(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "runtime.db"
            registry = PipelineRegistry.from_directory(
                Path(__file__).resolve().parents[1]
                / "examples"
                / "pipelines"
            )
            service = RuntimeService(
                registry=registry,
                store=SQLiteStateStore(db_path),
            )

            async def seed() -> None:
                await service.create_run_with_documents(
                    RuntimeRunInput(
                        run_id="run_stuck",
                        pipeline_id="basic_enrichment",
                        documents=[
                            RuntimeDocumentInput(
                                document_id="doc_slow",
                                values={"source": "slow.txt"},
                            )
                        ],
                    )
                )

            asyncio.run(seed())

            app = FastAPI()
            app.include_router(create_runtime_router(service), prefix="/api")

            async def exercise_api() -> None:
                async with ProcessRuntimeClient(
                    "http://runtime.test",
                    transport=httpx.ASGITransport(app=app),
                ) as client:
                    queued = await client.stuck_work_page(
                        run_id="run_stuck",
                        status=ProcessStatus.queued.value,
                        queued_after_seconds=0,
                    )
                    self.assertEqual(queued.count, 1)
                    self.assertEqual(queued.warning_count, 1)
                    self.assertEqual(queued.critical_count, 0)
                    self.assertEqual(queued.filters["status"], "queued")
                    self.assertEqual(
                        queued.filters["queued_after_seconds"],
                        0.0,
                    )
                    self.assertEqual(queued.items[0].document_id, "doc_slow")
                    self.assertEqual(queued.items[0].process_id, "ingest")
                    self.assertEqual(queued.items[0].operation_type, "ingest")
                    self.assertEqual(queued.items[0].reason, "queued_too_long")
                    self.assertEqual(queued.items[0].severity, "warning")
                    self.assertEqual(
                        queued.items[0].suggested_actions,
                        [
                            ProcessAction.retry,
                            ProcessAction.skip,
                            ProcessAction.cancel,
                        ],
                    )

                    claim = await client.claim_next(
                        run_id="run_stuck",
                        pipeline_id="basic_enrichment",
                        worker_id="slow-worker",
                        lease_seconds=0.1,
                    )
                    self.assertIsNotNone(claim)
                    assert claim is not None
                    self.assertEqual(claim.process.id, "ingest")
                    await asyncio.sleep(0.15)

                    expired = await client.stuck_work_page(
                        run_id="run_stuck",
                        status=ProcessStatus.running.value,
                        running_after_seconds=999,
                    )
                    self.assertEqual(expired.count, 1)
                    self.assertEqual(expired.warning_count, 0)
                    self.assertEqual(expired.critical_count, 1)
                    item = expired.items[0]
                    self.assertEqual(item.reason, "claim_expired")
                    self.assertEqual(item.severity, "critical")
                    self.assertEqual(item.worker_id, "slow-worker")
                    self.assertEqual(item.attempt, 1)
                    self.assertTrue(item.claim_expires_at)
                    self.assertIn("overdue_seconds", item.data)
                    self.assertEqual(
                        item.suggested_actions,
                        [ProcessAction.cancel, ProcessAction.retry],
                    )

                    listed = await client.list_stuck_work(
                        run_id="run_stuck",
                        status=ProcessStatus.running.value,
                        operation_type="ingest",
                        running_after_seconds=999,
                    )
                    self.assertEqual(len(listed), 1)
                    self.assertEqual(listed[0]["operation_type"], "ingest")
                    self.assertEqual(listed[0]["reason"], "claim_expired")

            asyncio.run(exercise_api())

            listed_cli = _run_cli(
                "--pipeline-dir",
                "examples/pipelines",
                "stuck-work",
                "--db",
                str(db_path),
                "--run-id",
                "run_stuck",
                "--status",
                ProcessStatus.running.value,
                "--operation-type",
                "ingest",
                "--running-after-seconds",
                "999",
            )
            self.assertTrue(listed_cli["ok"])
            self.assertEqual(listed_cli["stuck_work"]["count"], 1)
            self.assertEqual(
                listed_cli["stuck_work"]["items"][0]["reason"],
                "claim_expired",
            )

    def test_runtime_stuck_work_uses_step_sla_policy_thresholds(self) -> None:
        pipeline = PipelineSpec(
            id="sla_flow",
            steps=[
                ProcessSpec(
                    id="ingest",
                    adapter=AdapterSpec(kind="queue", queue="sla.ingest"),
                    sla={"queued_after_seconds": 0},
                )
            ],
        )
        service = RuntimeService(
            registry=PipelineRegistry([pipeline]),
            store=InMemoryStateStore(),
        )

        async def exercise() -> None:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_sla",
                    pipeline_id="sla_flow",
                    documents=[RuntimeDocumentInput(document_id="doc_sla")],
                )
            )
            default_filtered = await service.stuck_work(
                "run_sla",
                status=ProcessStatus.queued,
                queued_after_seconds=999,
            )
            self.assertEqual(default_filtered.count, 1)
            item = default_filtered.items[0]
            self.assertEqual(item.reason, "queued_too_long")
            self.assertEqual(item.threshold_seconds, 0)
            self.assertEqual(item.process.sla.queued_after_seconds, 0)

        asyncio.run(exercise())

    def test_runtime_append_documents_reopens_completed_run(self) -> None:
        pipeline = PipelineSpec(
            id="append_reopen",
            steps=[
                ProcessSpec(
                    id="ingest",
                    adapter=AdapterSpec(kind="queue", queue="append.ingest"),
                )
            ],
        )
        store = InMemoryStateStore()
        service = RuntimeService(registry=PipelineRegistry([pipeline]), store=store)

        async def exercise() -> None:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_append_reopen",
                    pipeline_id="append_reopen",
                    documents=[RuntimeDocumentInput(document_id="doc_1")],
                )
            )
            await service.complete_process_output(
                run_id="run_append_reopen",
                document_id="doc_1",
                process_id="ingest",
                pipeline_id="append_reopen",
                output=ProcessOutput(values={"done": True}),
            )
            completed = await service.get_run("run_append_reopen")
            self.assertIsNotNone(completed)
            assert completed is not None
            self.assertEqual(completed.status, RunStatus.completed)

            reopened, schedules = await service.append_run_documents(
                run_id="run_append_reopen",
                pipeline_id="append_reopen",
                documents=[RuntimeDocumentInput(document_id="doc_2")],
            )
            self.assertEqual(reopened.status, RunStatus.queued)
            self.assertEqual([schedule.document_id for schedule in schedules], ["doc_2"])
            documents = await store.list_document_records(run_id="run_append_reopen")
            self.assertEqual(
                [document.document_id for document in documents],
                ["doc_1", "doc_2"],
            )

        asyncio.run(exercise())

    def test_runtime_rejects_document_type_not_accepted_by_package_pipeline(self) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="pkg",
                pipelines=["invoice.yaml"],
                document_types=[
                    DocumentTypeSpec(id="invoice_document"),
                    DocumentTypeSpec(id="email_document"),
                ],
                capabilities=[
                    CapabilitySpec(
                        id="ingest_invoice",
                        accepts_document_types=["invoice_document"],
                    )
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="invoice_flow",
                steps=[
                    ProcessSpec(
                        id="ingest",
                        capability="ingest_invoice",
                        adapter=AdapterSpec(kind="queue", queue="invoice.ingest"),
                    )
                ],
            ),
            package_id="pkg",
        )
        registry.validate_package_workers("pkg")
        store = InMemoryStateStore()
        service = RuntimeService(registry=registry, store=store)

        async def run_api() -> None:
            with self.assertRaisesRegex(
                ValueError,
                "type 'email_document'.*is not accepted by pipeline 'invoice_flow'",
            ):
                await service.create_run_with_documents(
                    RuntimeRunInput(
                        run_id="run_bad_doc_type",
                        pipeline_id="invoice_flow",
                        documents=[
                            RuntimeDocumentInput(
                                document_id="email_1",
                                document_type="email_document",
                                source_uri="file:///tmp/email.eml",
                            )
                        ],
                    )
                )
            self.assertIsNone(await store.get_run("run_bad_doc_type"))

        asyncio.run(run_api())

    def test_runtime_rejects_document_metadata_not_matching_document_type_schema(self) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="pkg",
                pipelines=["case.yaml"],
                document_types=[
                    DocumentTypeSpec(
                        id="case_document",
                        metadata_schema={
                            "type": "object",
                            "required": ["case_id"],
                            "properties": {"case_id": {"type": "string"}},
                            "additionalProperties": False,
                        },
                    )
                ],
                capabilities=[
                    CapabilitySpec(
                        id="ingest_case",
                        accepts_document_types=["case_document"],
                    )
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="case_flow",
                steps=[
                    ProcessSpec(
                        id="ingest",
                        capability="ingest_case",
                        adapter=AdapterSpec(kind="queue", queue="case.ingest"),
                    )
                ],
            ),
            package_id="pkg",
        )
        registry.validate_package_workers("pkg")
        store = InMemoryStateStore()
        service = RuntimeService(registry=registry, store=store)

        async def run_api() -> None:
            with self.assertRaisesRegex(
                ValueError,
                "metadata for type 'case_document'.*'case_id' is a required property",
            ):
                await service.create_run_with_documents(
                    RuntimeRunInput(
                        run_id="run_bad_doc_metadata",
                        pipeline_id="case_flow",
                        documents=[
                            RuntimeDocumentInput(
                                document_id="case.pdf",
                                document_type="case_document",
                                metadata={"mailbox": "ops"},
                            )
                        ],
                    )
                )
            self.assertIsNone(await store.get_run("run_bad_doc_metadata"))

        asyncio.run(run_api())

    def test_runtime_rejects_document_values_not_matching_document_type_schema(self) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="pkg",
                pipelines=["case.yaml"],
                document_types=[
                    DocumentTypeSpec(
                        id="case_document",
                        value_schema={
                            "type": "object",
                            "required": ["case_id"],
                            "properties": {"case_id": {"type": "string"}},
                            "additionalProperties": False,
                        },
                    )
                ],
                capabilities=[
                    CapabilitySpec(
                        id="ingest_case",
                        accepts_document_types=["case_document"],
                    )
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="case_flow",
                steps=[
                    ProcessSpec(
                        id="ingest",
                        capability="ingest_case",
                        adapter=AdapterSpec(kind="queue", queue="case.ingest"),
                    )
                ],
            ),
            package_id="pkg",
        )
        registry.validate_package_workers("pkg")
        store = InMemoryStateStore()
        service = RuntimeService(registry=registry, store=store)

        async def run_api() -> None:
            with self.assertRaisesRegex(
                ValueError,
                "values for type 'case_document'.*'case_id' is a required property",
            ):
                await service.create_run_with_documents(
                    RuntimeRunInput(
                        run_id="run_bad_doc_values",
                        pipeline_id="case_flow",
                        documents=[
                            RuntimeDocumentInput(
                                document_id="case.pdf",
                                document_type="case_document",
                                values={"mailbox": "ops"},
                            )
                        ],
                    )
                )
            self.assertIsNone(await store.get_run("run_bad_doc_values"))

        asyncio.run(run_api())

    def test_runtime_rejects_document_media_type_not_matching_document_type(self) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="pkg",
                pipelines=["invoice.yaml"],
                document_types=[
                    DocumentTypeSpec(
                        id="invoice_document",
                        media_types=["application/pdf"],
                    )
                ],
                capabilities=[
                    CapabilitySpec(
                        id="ingest_invoice",
                        accepts_document_types=["invoice_document"],
                    )
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="invoice_flow",
                steps=[
                    ProcessSpec(
                        id="ingest",
                        capability="ingest_invoice",
                        adapter=AdapterSpec(kind="queue", queue="invoice.ingest"),
                    )
                ],
            ),
            package_id="pkg",
        )
        registry.validate_package_workers("pkg")
        store = InMemoryStateStore()
        service = RuntimeService(registry=registry, store=store)

        async def run_api() -> None:
            with self.assertRaisesRegex(
                ValueError,
                "media type 'text/plain'.*document type 'invoice_document'",
            ):
                await service.create_run_with_documents(
                    RuntimeRunInput(
                        run_id="run_bad_doc_media",
                        pipeline_id="invoice_flow",
                        documents=[
                            RuntimeDocumentInput(
                                document_id="invoice.txt",
                                document_type="invoice_document",
                                media_type="text/plain",
                                source_uri="file:///tmp/invoice.txt",
                            )
                        ],
                    )
                )
            self.assertIsNone(await store.get_run("run_bad_doc_media"))

        asyncio.run(run_api())

    def test_runtime_rejects_document_extension_not_matching_document_type(self) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="pkg",
                pipelines=["invoice.yaml"],
                document_types=[
                    DocumentTypeSpec(
                        id="invoice_document",
                        media_types=["application/pdf"],
                        extensions=[".pdf"],
                    )
                ],
                capabilities=[
                    CapabilitySpec(
                        id="ingest_invoice",
                        accepts_document_types=["invoice_document"],
                    )
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="invoice_flow",
                steps=[
                    ProcessSpec(
                        id="ingest",
                        capability="ingest_invoice",
                        adapter=AdapterSpec(kind="queue", queue="invoice.ingest"),
                    )
                ],
            ),
            package_id="pkg",
        )
        registry.validate_package_workers("pkg")
        store = InMemoryStateStore()
        service = RuntimeService(registry=registry, store=store)

        async def run_api() -> None:
            with self.assertRaisesRegex(
                ValueError,
                "extension '.txt'.*document type 'invoice_document'",
            ):
                await service.create_run_with_documents(
                    RuntimeRunInput(
                        run_id="run_bad_doc_extension",
                        pipeline_id="invoice_flow",
                        documents=[
                            RuntimeDocumentInput(
                                document_id="invoice.txt",
                                document_type="invoice_document",
                                media_type="application/pdf",
                                source_uri="file:///tmp/invoice.txt",
                            )
                        ],
                    )
                )
            self.assertIsNone(await store.get_run("run_bad_doc_extension"))

        asyncio.run(run_api())

    def test_runtime_cli_validates_run_input_without_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "case_package"
            package_dir.mkdir()
            source = root / "case.pdf"
            source.write_bytes(b"%PDF-1.7\n")
            (package_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: case_package
                    pipelines:
                      - case.yaml
                    document_types:
                      - id: case_document
                        media_types: ["application/pdf"]
                        extensions: [".pdf"]
                        value_schema:
                          type: object
                          required: ["case_id"]
                          properties:
                            case_id:
                              type: string
                          additionalProperties: true
                        metadata_schema:
                          type: object
                          required: ["case_id"]
                          properties:
                            case_id:
                              type: string
                          additionalProperties: true
                    capabilities:
                      - id: ingest_case
                        accepts_document_types: [case_document]
                    workers:
                      - id: ingest_worker
                        capabilities: [ingest_case]
                        pipeline: case_flow
                        process: ingest
                        command: ["python", "workers/ingest.py"]
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "case.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: case_flow
                    steps:
                      - id: ingest
                        capability: ingest_case
                        adapter:
                          kind: queue
                          queue: case.ingest
                    """
                ).strip(),
                encoding="utf-8",
            )

            preview = _run_cli(
                "--pipeline-dir",
                str(root),
                "validate-run",
                "--pipeline",
                "case_flow",
                "--run-id",
                "run_cli_preview",
                "--document-type",
                "case_document",
                "--media-type",
                "application/pdf",
                "--file",
                str(source),
                "--value",
                "case_id=C-1",
                "--metadata",
                "case_id=C-1",
            )
            self.assertTrue(preview["ok"])
            self.assertEqual(preview["document_count"], 1)
            self.assertEqual(preview["documents"][0]["document_id"], "case.pdf")
            self.assertEqual(preview["documents"][0]["metadata_keys"], ["case_id", "source_path"])
            self.assertEqual(
                preview["contracts"]["case_flow"]["steps"][0]["capability"]["id"],
                "ingest_case",
            )

            plan = _run_cli(
                "--pipeline-dir",
                str(root),
                "plan-run",
                "--pipeline",
                "case_flow",
                "--run-id",
                "run_cli_plan",
                "--document-type",
                "case_document",
                "--media-type",
                "application/pdf",
                "--file",
                str(source),
                "--value",
                "case_id=C-1",
                "--metadata",
                "case_id=C-1",
            )
            self.assertTrue(plan["ok"])
            self.assertEqual(plan["plan"]["process_instance_count"], 1)
            self.assertEqual(plan["plan"]["queued_count"], 1)
            self.assertEqual(plan["plan"]["waiting_count"], 0)
            self.assertEqual(
                plan["plan"]["processes"][0]["declared_worker_ids"],
                ["ingest_worker"],
            )
            self.assertEqual(
                plan["plan"]["worker_demands"][0]["initial_target_worker_count"],
                1,
            )
            self.assertEqual(
                plan["plan"]["documents"][0]["queued"][0]["process_id"],
                "ingest",
            )

            code, invalid = _run_cli_raw(
                "--pipeline-dir",
                str(root),
                "validate-run",
                "--pipeline",
                "case_flow",
                "--document-type",
                "case_document",
                "--media-type",
                "application/pdf",
                "--file",
                str(source),
            )
            self.assertEqual(code, 1)
            self.assertFalse(invalid["ok"])
            self.assertIn("'case_id' is a required property", invalid["error"])

    def test_runtime_cli_creates_run_from_yaml_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipeline_dir = root / "pipelines"
            pipeline_dir.mkdir()
            (pipeline_dir / "batch.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: manifest_flow
                    steps:
                      - id: ingest
                        adapter:
                          kind: queue
                          queue: manifest.ingest
                    """
                ).strip(),
                encoding="utf-8",
            )
            manifest = root / "run-input.yaml"
            manifest.write_text(
                textwrap.dedent(
                    """
                    run_id: run_from_manifest
                    title: Manifest batch
                    pipeline_id: manifest_flow
                    config:
                      resource_pools:
                        default:
                          units:
                            manifest_slots: 1
                    documents:
                      - document_id: manifest_doc
                        title: Manifest doc
                        source_uri: file:///tmp/manifest.txt
                        values:
                          case_id: M
                        metadata:
                          origin: manifest
                    """
                ).strip(),
                encoding="utf-8",
            )

            preview = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "validate-run",
                "--run-input",
                str(manifest),
                "--existing-run",
                "resume",
                "--existing-document",
                "reuse",
                "--document",
                "cli_doc=file:///tmp/cli.txt",
                "--value",
                "source=cli",
                "--resource-pool",
                "default.units.cli_slots=2",
            )
            self.assertTrue(preview["ok"])
            self.assertEqual(preview["run_id"], "run_from_manifest")
            self.assertEqual(preview["pipeline_id"], "manifest_flow")
            self.assertEqual(preview["existing_run_policy"], "resume")
            self.assertEqual(preview["existing_document_policy"], "reuse")
            self.assertEqual(
                [item["document_id"] for item in preview["documents"]],
                ["manifest_doc", "cli_doc"],
            )
            self.assertEqual(preview["document_summary"]["document_count"], 2)
            self.assertEqual(
                preview["document_summary"]["pipeline_counts"],
                {"manifest_flow": 2},
            )
            self.assertEqual(
                preview["document_summary"]["source_scheme_counts"],
                {"file": 2},
            )
            self.assertEqual(
                preview["document_summary"]["value_keys"],
                ["case_id", "source"],
            )
            self.assertEqual(
                preview["document_summary"]["metadata_keys"],
                ["origin"],
            )
            self.assertEqual(
                preview["document_summary"]["missing_document_type_count"],
                2,
            )

            db_path = root / "runtime.db"
            created = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "create-run",
                "--db",
                str(db_path),
                "--run-input",
                str(manifest),
                "--document",
                "cli_doc=file:///tmp/cli.txt",
                "--value",
                "source=cli",
                "--resource-pool",
                "default.units.cli_slots=2",
            )
            self.assertEqual(created["run"]["id"], "run_from_manifest")
            self.assertEqual(created["run"]["status"], "queued")
            self.assertEqual(created["document_count"], 2)
            self.assertEqual(
                created["run"]["config"]["resource_pools"]["default"]["units"],
                {"manifest_slots": 1.0, "cli_slots": 2.0},
            )

            reopened = SQLiteStateStore(db_path)
            manifest_input = asyncio.run(
                reopened.get_document_input(
                    run_id="run_from_manifest",
                    document_id="manifest_doc",
                )
            )
            cli_input = asyncio.run(
                reopened.get_document_input(
                    run_id="run_from_manifest",
                    document_id="cli_doc",
                )
            )
            self.assertIsNotNone(manifest_input)
            self.assertIsNotNone(cli_input)
            assert manifest_input is not None
            assert cli_input is not None
            self.assertEqual(manifest_input.values["case_id"], "M")
            self.assertEqual(cli_input.values["source"], "cli")

    def test_runtime_cli_inspects_manifest_duplicates_without_validating_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            digest = hashlib.sha256(b"same").hexdigest()
            manifest = root / "duplicates.yaml"
            manifest.write_text(
                textwrap.dedent(
                    f"""
                    run_id: run_duplicates
                    pipeline_id: duplicate_flow
                    documents:
                      - invalid-document
                      - document_id: duplicate_doc
                        source_uri: s3://bucket/a.pdf
                        media_type: application/pdf
                        metadata:
                          source_sha256: {digest}
                      - document_id: duplicate_doc
                        source_uri: s3://bucket/a.pdf
                        document_type: generic_document
                        media_type: application/pdf
                        metadata:
                          source_sha256: sha256:{digest}
                    """
                ).strip(),
                encoding="utf-8",
            )

            code, inspected = _run_cli_raw(
                "inspect-run-input",
                "--run-input",
                str(manifest),
            )
            self.assertEqual(code, 1)
            self.assertFalse(inspected["ok"])
            self.assertEqual(inspected["error_count"], 2)
            self.assertEqual(inspected["warning_count"], 2)
            self.assertEqual(inspected["document_count"], 3)
            self.assertEqual(inspected["document_summary"]["document_count"], 2)
            self.assertEqual(
                inspected["document_summary"]["document_type_counts"],
                {"generic_document": 1},
            )
            self.assertEqual(
                inspected["document_summary"]["missing_document_type_count"],
                1,
            )
            self.assertEqual(
                [issue["type"] for issue in inspected["issues"]],
                [
                    "invalid_document",
                    "duplicate_document_id",
                    "duplicate_source_sha256",
                    "duplicate_source_uri",
                ],
            )
            self.assertEqual(inspected["issues"][1]["document_id"], "duplicate_doc")
            self.assertEqual(inspected["issues"][2]["source_sha256"], digest)
            self.assertEqual(
                inspected["issues"][2]["document_ids"],
                ["duplicate_doc", "duplicate_doc"],
            )

    def test_runtime_cli_discovers_documents_as_run_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_dir = root / "incoming"
            nested = source_dir / "nested"
            nested.mkdir(parents=True)
            (source_dir / "a.txt").write_text("alpha", encoding="utf-8")
            (nested / "b.pdf").write_bytes(b"%PDF-1.7\n")
            (source_dir / "skip.tmp").write_text("skip", encoding="utf-8")

            manifest = _run_cli(
                "discover-documents",
                "--input-dir",
                str(source_dir),
                "--pipeline",
                "discovered_flow",
                "--run-id",
                "run_discovered",
                "--title",
                "Discovered batch",
                "--document-type",
                "generic_document",
                "--include",
                "*.txt",
                "--include",
                "nested/*.pdf",
                "--exclude",
                "*.tmp",
                "--value",
                "batch=local",
                "--metadata",
                "origin=discovery",
            )
            self.assertEqual(manifest["run_id"], "run_discovered")
            self.assertEqual(manifest["pipeline_id"], "discovered_flow")
            self.assertEqual(manifest["title"], "Discovered batch")
            self.assertEqual(
                [document["document_id"] for document in manifest["documents"]],
                ["a.txt", "nested/b.pdf"],
            )
            self.assertEqual(manifest["documents"][0]["media_type"], "text/plain")
            self.assertEqual(manifest["documents"][1]["media_type"], "application/pdf")
            self.assertEqual(manifest["documents"][0]["values"]["batch"], "local")
            self.assertEqual(manifest["documents"][0]["metadata"]["origin"], "discovery")
            self.assertIn("source_size", manifest["documents"][0]["metadata"])
            self.assertIn("source_mtime", manifest["documents"][0]["metadata"])

            pipeline_dir = root / "pipelines"
            pipeline_dir.mkdir()
            (pipeline_dir / "discovered.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: discovered_flow
                    steps:
                      - id: ingest
                        adapter:
                          kind: queue
                          queue: discovered.ingest
                    """
                ).strip(),
                encoding="utf-8",
            )
            manifest_path = root / "run-input.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            db_path = root / "runtime.db"
            created = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "create-run",
                "--db",
                str(db_path),
                "--run-input",
                str(manifest_path),
            )
            self.assertEqual(created["run"]["id"], "run_discovered")
            self.assertEqual(created["document_count"], 2)
            self.assertEqual(created["schedules"][0]["queued"][0]["id"], "ingest")

    def test_runtime_cli_discovers_documents_with_route_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_dir = root / "incoming"
            source_dir.mkdir()
            (source_dir / "invoice.pdf").write_bytes(b"%PDF-1.7\n")
            (source_dir / "mail.eml").write_text("From: test@example.com\n", encoding="utf-8")
            route = root / "routes.yaml"
            route.write_text(
                textwrap.dedent(
                    """
                    routes:
                      - id: invoice_documents
                        match:
                          extensions: [.pdf]
                        set:
                          pipeline_id: invoice_flow
                          document_type: invoice_document
                          metadata:
                            route: invoice_documents
                      - id: email_documents
                        match:
                          extensions: [.eml]
                        set:
                          pipeline: email_flow
                          document_type: email_document
                          values:
                            source_kind: email
                    """
                ).strip(),
                encoding="utf-8",
            )

            route_report_path = root / "route-report.json"
            manifest = _run_cli(
                "discover-documents",
                "--input-dir",
                str(source_dir),
                "--route",
                str(route),
                "--route-report",
                str(route_report_path),
                "--run-id",
                "run_routed",
                "--include",
                "*.pdf",
                "--include",
                "*.eml",
            )
            self.assertNotIn("pipeline_id", manifest)
            self.assertEqual(
                [document["pipeline_id"] for document in manifest["documents"]],
                ["invoice_flow", "email_flow"],
            )
            invoice, email = manifest["documents"]
            self.assertEqual(invoice["document_type"], "invoice_document")
            self.assertEqual(invoice["metadata"]["route"], "invoice_documents")
            self.assertEqual(email["document_type"], "email_document")
            self.assertEqual(email["values"]["source_kind"], "email")
            route_report = json.loads(route_report_path.read_text(encoding="utf-8"))
            self.assertEqual(route_report["document_count"], 2)
            self.assertEqual(route_report["routed_count"], 2)
            self.assertEqual(route_report["candidate_count"], 3)
            email_report = next(
                item
                for item in route_report["documents"]
                if item["document_id"] == "mail.eml"
            )
            self.assertEqual(email_report["matched_candidate_count"], 1)
            self.assertEqual(
                email_report["unmatched_reasons"][0]["route_id"],
                "invoice_documents",
            )
            self.assertEqual(
                email_report["unmatched_reasons"][0]["reasons"][0]["reason"],
                "extension_mismatch",
            )

            pipeline_dir = root / "pipelines"
            pipeline_dir.mkdir()
            (pipeline_dir / "invoice.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: invoice_flow
                    steps:
                      - id: ingest_invoice
                        adapter:
                          kind: queue
                          queue: invoice.ingest
                    """
                ).strip(),
                encoding="utf-8",
            )
            (pipeline_dir / "email.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: email_flow
                    steps:
                      - id: ingest_email
                        adapter:
                          kind: queue
                          queue: email.ingest
                    """
                ).strip(),
                encoding="utf-8",
            )
            manifest_path = root / "run-input.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            created = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "create-run",
                "--db",
                str(root / "runtime.db"),
                "--run-input",
                str(manifest_path),
            )
            self.assertEqual(created["run"]["id"], "run_routed")
            self.assertEqual(created["document_count"], 2)
            self.assertEqual(
                sorted(schedule["pipeline_id"] for schedule in created["schedules"]),
                ["email_flow", "invoice_flow"],
            )

    def test_runtime_cli_auto_routes_documents_from_package_contracts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_dir = root / "incoming"
            source_dir.mkdir()
            (source_dir / "invoice.pdf").write_bytes(b"%PDF-1.7\n")
            (source_dir / "mail.eml").write_text("From: test@example.com\n", encoding="utf-8")
            pipeline_root = root / "pipelines"
            pipeline_dir = pipeline_root / "mixed"
            pipeline_dir.mkdir(parents=True)
            (pipeline_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: mixed_documents
                    document_types:
                      - id: invoice_document
                        media_types: [application/pdf]
                        extensions: [.pdf]
                      - id: email_document
                        media_types: [message/rfc822]
                        extensions: [.eml]
                    artifact_kinds:
                      - id: invoice_source
                      - id: email_source
                    capabilities:
                      - id: ingest_invoice
                        accepts_document_types: [invoice_document]
                        emits_artifact_kinds: [invoice_source]
                      - id: ingest_email
                        accepts_document_types: [email_document]
                        emits_artifact_kinds: [email_source]
                    pipelines:
                      - invoice.yaml
                      - email.yaml
                    """
                ).strip(),
                encoding="utf-8",
            )
            (pipeline_dir / "invoice.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: invoice_flow
                    steps:
                      - id: ingest_invoice
                        capability: ingest_invoice
                        adapter:
                          kind: queue
                          queue: invoice.ingest
                    """
                ).strip(),
                encoding="utf-8",
            )
            (pipeline_dir / "email.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: email_flow
                    steps:
                      - id: ingest_email
                        capability: ingest_email
                        adapter:
                          kind: queue
                          queue: email.ingest
                    """
                ).strip(),
                encoding="utf-8",
            )

            manifest = _run_cli(
                "--pipeline-dir",
                str(pipeline_root),
                "discover-documents",
                "--input-dir",
                str(source_dir),
                "--auto-route",
                "--run-id",
                "run_auto_routed",
            )
            self.assertNotIn("pipeline_id", manifest)
            self.assertEqual(
                {
                    document["document_id"]: (
                        document["pipeline_id"],
                        document["document_type"],
                    )
                    for document in manifest["documents"]
                },
                {
                    "invoice.pdf": ("invoice_flow", "invoice_document"),
                    "mail.eml": ("email_flow", "email_document"),
                },
            )

            manifest_path = root / "run-input.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            created = _run_cli(
                "--pipeline-dir",
                str(pipeline_root),
                "create-run",
                "--db",
                str(root / "runtime.db"),
                "--run-input",
                str(manifest_path),
            )
            self.assertEqual(created["document_count"], 2)
            self.assertEqual(
                sorted(schedule["pipeline_id"] for schedule in created["schedules"]),
                ["email_flow", "invoice_flow"],
            )

    def test_runtime_cli_auto_route_rejects_ambiguous_document_contracts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_dir = root / "incoming"
            source_dir.mkdir()
            (source_dir / "invoice.pdf").write_bytes(b"%PDF-1.7\n")
            pipeline_root = root / "pipelines"
            pipeline_dir = pipeline_root / "ambiguous"
            pipeline_dir.mkdir(parents=True)
            (pipeline_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: ambiguous_documents
                    document_types:
                      - id: invoice_document
                        media_types: [application/pdf]
                        extensions: [.pdf]
                    artifact_kinds:
                      - id: invoice_source
                    capabilities:
                      - id: ingest_invoice
                        accepts_document_types: [invoice_document]
                        emits_artifact_kinds: [invoice_source]
                    pipelines:
                      - invoice.yaml
                      - invoice_backup.yaml
                    """
                ).strip(),
                encoding="utf-8",
            )
            for name, pipeline_id, queue in [
                ("invoice.yaml", "invoice_flow", "invoice.ingest"),
                ("invoice_backup.yaml", "invoice_backup_flow", "invoice.backup"),
            ]:
                (pipeline_dir / name).write_text(
                    textwrap.dedent(
                        f"""
                        pipeline: {pipeline_id}
                        steps:
                          - id: ingest_invoice
                            capability: ingest_invoice
                            adapter:
                              kind: queue
                              queue: {queue}
                        """
                    ).strip(),
                    encoding="utf-8",
                )

            code, payload = _run_cli_raw(
                "--pipeline-dir",
                str(pipeline_root),
                "discover-documents",
                "--input-dir",
                str(source_dir),
                "--auto-route",
            )
            self.assertEqual(code, 1)
            self.assertFalse(payload["ok"])
            self.assertIn("Ambiguous auto-route", payload["error"])
            self.assertIn("invoice_backup_flow:invoice_document", payload["error"])

    def test_runtime_cli_auto_routes_source_list_document_type_to_pipeline(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipeline_root = root / "pipelines"
            pipeline_dir = pipeline_root / "typed"
            pipeline_dir.mkdir(parents=True)
            (pipeline_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: typed_documents
                    document_types:
                      - id: invoice_document
                        media_types: [application/pdf]
                        extensions: [.pdf]
                    artifact_kinds:
                      - id: invoice_source
                    capabilities:
                      - id: ingest_invoice
                        accepts_document_types: [invoice_document]
                        emits_artifact_kinds: [invoice_source]
                    pipelines:
                      - invoice.yaml
                    """
                ).strip(),
                encoding="utf-8",
            )
            (pipeline_dir / "invoice.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: invoice_flow
                    steps:
                      - id: ingest_invoice
                        capability: ingest_invoice
                        adapter:
                          kind: queue
                          queue: invoice.ingest
                    """
                ).strip(),
                encoding="utf-8",
            )
            source_list = root / "sources.csv"
            source_list.write_text(
                "\n".join(
                    [
                        "document_id,source_uri,document_type,media_type",
                        "typed_doc,s3://bucket/blob.bin,invoice_document,application/octet-stream",
                    ]
                ),
                encoding="utf-8",
            )

            manifest = _run_cli(
                "--pipeline-dir",
                str(pipeline_root),
                "discover-documents",
                "--source-list",
                str(source_list),
                "--auto-route",
            )
            self.assertNotIn("pipeline_id", manifest)
            self.assertEqual(
                manifest["documents"][0]["pipeline_id"],
                "invoice_flow",
            )
            self.assertEqual(
                manifest["documents"][0]["document_type"],
                "invoice_document",
            )

    def test_runtime_cli_discovers_documents_from_source_list(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_dir = root / "incoming"
            source_dir.mkdir()
            (source_dir / "local.pdf").write_bytes(b"%PDF-1.7\n")
            source_list = root / "sources.csv"
            source_list.write_text(
                "\n".join(
                    [
                        "document_id,title,path,source_uri,document_type,media_type,value.case_id,metadata.mailbox",
                        "local_doc,Local PDF,incoming/local.pdf,,invoice_document,,C-1,ops",
                        "remote_doc,Remote email,,s3://bucket/mail.eml,email_document,message/rfc822,C-2,sales",
                        "skip_doc,Skip,,s3://bucket/skip.tmp,generic_document,text/plain,C-3,tmp",
                    ]
                ),
                encoding="utf-8",
            )

            manifest = _run_cli(
                "discover-documents",
                "--source-list",
                str(source_list),
                "--pipeline",
                "source_list_flow",
                "--run-id",
                "run_source_list",
                "--include",
                "*_doc",
                "--exclude",
                "skip_*",
                "--value",
                "batch=csv",
                "--metadata",
                "origin=source-list",
            )
            self.assertEqual(
                [document["document_id"] for document in manifest["documents"]],
                ["local_doc", "remote_doc"],
            )
            local_doc, remote_doc = manifest["documents"]
            self.assertEqual(local_doc["source_uri"], (source_dir / "local.pdf").resolve().as_uri())
            self.assertEqual(local_doc["media_type"], "application/pdf")
            self.assertEqual(local_doc["document_type"], "invoice_document")
            self.assertEqual(local_doc["values"], {"batch": "csv", "case_id": "C-1"})
            self.assertEqual(local_doc["metadata"]["mailbox"], "ops")
            self.assertEqual(local_doc["metadata"]["origin"], "source-list")
            self.assertEqual(local_doc["metadata"]["source_list_row"], 2)
            self.assertEqual(remote_doc["source_uri"], "s3://bucket/mail.eml")
            self.assertEqual(remote_doc["media_type"], "message/rfc822")
            self.assertEqual(remote_doc["document_type"], "email_document")
            self.assertEqual(remote_doc["values"]["case_id"], "C-2")

    def test_runtime_cli_discovers_source_fingerprints_and_sha_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_dir = root / "incoming"
            source_dir.mkdir()
            payload = b"stable document"
            source = source_dir / "doc.txt"
            source.write_bytes(payload)
            digest = hashlib.sha256(payload).hexdigest()

            manifest = _run_cli(
                "discover-documents",
                "--input-dir",
                str(source_dir),
                "--pipeline",
                "fingerprint_flow",
                "--run-id",
                "run_fingerprint",
                "--content-hash",
                "--document-id-mode",
                "sha256",
            )
            self.assertEqual(manifest["documents"][0]["document_id"], f"sha256:{digest}")
            self.assertEqual(manifest["documents"][0]["metadata"]["source_sha256"], digest)

            source_list = root / "sources.csv"
            source_list.write_text(
                "\n".join(
                    [
                        "source_uri,source_sha256,title",
                        f"s3://bucket/remote.pdf,{digest},Remote PDF",
                    ]
                ),
                encoding="utf-8",
            )
            source_manifest = _run_cli(
                "discover-documents",
                "--source-list",
                str(source_list),
                "--pipeline",
                "fingerprint_flow",
                "--document-id-mode",
                "sha256",
            )
            self.assertEqual(
                source_manifest["documents"][0]["document_id"],
                f"sha256:{digest}",
            )
            self.assertEqual(
                source_manifest["documents"][0]["metadata"]["source_sha256"],
                digest,
            )

    def test_runtime_cli_create_run_initializes_file_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipeline_dir = root / "pipelines"
            pipeline_dir.mkdir()
            source = root / "doc.txt"
            source.write_text("hello", encoding="utf-8")
            (pipeline_dir / "batch.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: batch_flow
                    steps:
                      - id: ingest
                        adapter:
                          kind: queue
                          queue: batch.ingest
                    """
                ).strip(),
                encoding="utf-8",
            )
            db_path = root / "runtime.db"

            created = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "create-run",
                "--db",
                str(db_path),
                "--pipeline",
                "batch_flow",
                "--run-id",
                "run_cli_batch",
                "--title",
                "CLI batch",
                "--document-type",
                "generic_document",
                "--media-type",
                "text/plain",
                "--file",
                str(source),
                "--value",
                "case_id=C",
                "--metadata",
                "origin=fixture",
            )

            self.assertTrue(created["ok"])
            self.assertEqual(created["document_count"], 1)
            self.assertEqual(created["run"]["status"], "queued")
            reopened = SQLiteStateStore(db_path)
            doc_input = asyncio.run(
                reopened.get_document_input(
                    run_id="run_cli_batch",
                    document_id="doc.txt",
                )
            )
            self.assertIsNotNone(doc_input)
            assert doc_input is not None
            self.assertEqual(doc_input.values["case_id"], "C")
            self.assertEqual(doc_input.values["document"]["metadata"]["origin"], "fixture")
            self.assertEqual(doc_input.artifacts[0].uri, source.resolve().as_uri())
            doc_record = asyncio.run(
                reopened.get_document(
                    run_id="run_cli_batch",
                    document_id="doc.txt",
                )
            )
            self.assertIsNotNone(doc_record)
            assert doc_record is not None
            self.assertEqual(doc_record.document_type, "generic_document")
            self.assertEqual(doc_record.media_type, "text/plain")
            self.assertEqual(doc_record.source_uri, source.resolve().as_uri())
            self.assertEqual(doc_record.metadata["origin"], "fixture")
            self.assertEqual(doc_record.status, RuntimeDocumentStatus.queued)

            buffer = StringIO()
            with redirect_stdout(buffer):
                code = runtime_cli_main(
                    [
                        "--pipeline-dir",
                        str(pipeline_dir),
                        "create-run",
                        "--db",
                        str(db_path),
                        "--pipeline",
                        "batch_flow",
                        "--run-id",
                        "run_cli_batch",
                        "--file",
                        str(source),
                    ]
                )
            self.assertEqual(code, 1)
            self.assertIn("already exists", buffer.getvalue())

            resumed = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "create-run",
                "--db",
                str(db_path),
                "--pipeline",
                "batch_flow",
                "--run-id",
                "run_cli_batch",
                "--existing-run",
                "resume",
                "--existing-document",
                "reuse",
                "--file",
                str(source),
                "--value",
                "case_id=changed",
                "--metadata",
                "origin=changed",
            )
            self.assertTrue(resumed["ok"])
            self.assertEqual(resumed["run"]["title"], "CLI batch")
            self.assertEqual(resumed["document_count"], 1)
            reopened = SQLiteStateStore(db_path)
            doc_input = asyncio.run(
                reopened.get_document_input(
                    run_id="run_cli_batch",
                    document_id="doc.txt",
                )
            )
            doc_record = asyncio.run(
                reopened.get_document(
                    run_id="run_cli_batch",
                    document_id="doc.txt",
                )
            )
            self.assertIsNotNone(doc_input)
            self.assertIsNotNone(doc_record)
            assert doc_input is not None
            assert doc_record is not None
            self.assertEqual(doc_input.values["case_id"], "C")
            self.assertEqual(doc_record.metadata["origin"], "fixture")

            preview = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "validate-run",
                "--pipeline",
                "batch_flow",
                "--run-id",
                "run_cli_batch",
                "--existing-run",
                "resume",
                "--existing-document",
                "reuse",
                "--file",
                str(source),
            )
            self.assertEqual(preview["existing_run_policy"], "resume")
            self.assertEqual(preview["existing_document_policy"], "reuse")

    def test_runtime_cli_reports_queue_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipeline_dir = root / "pipelines"
            pipeline_dir.mkdir()
            (pipeline_dir / "metrics.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: metrics_flow
                    steps:
                      - id: ocr
                        priority: 30
                        max_concurrency: 1
                        adapter:
                          kind: queue
                          queue: metrics.ocr
                      - id: export
                        needs: [ocr]
                        adapter:
                          kind: queue
                          queue: metrics.export
                    """
                ).strip(),
                encoding="utf-8",
            )
            db_path = root / "runtime.db"

            _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "create-run",
                "--db",
                str(db_path),
                "--pipeline",
                "metrics_flow",
                "--run-id",
                "run_cli_metrics",
                "--document",
                "doc_a=file:///tmp/doc_a.txt",
                "--document",
                "doc_b=file:///tmp/doc_b.txt",
            )
            _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "claim",
                "--db",
                str(db_path),
                "--pipeline",
                "metrics_flow",
                "--run-id",
                "run_cli_metrics",
                "--worker-id",
                "worker-ocr",
                "--adapter-kind",
                "queue",
            )
            metrics = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "queue-metrics",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli_metrics",
            )

            self.assertTrue(metrics["ok"])
            payload = metrics["metrics"]
            self.assertEqual(payload["queued_count"], 1)
            self.assertEqual(payload["running_count"], 1)
            self.assertEqual(payload["missing_worker_count"], 1)
            self.assertEqual(payload["saturated_process_count"], 1)
            ocr = next(
                item for item in payload["processes"] if item["process_id"] == "ocr"
            )
            self.assertEqual(ocr["queued_count"], 1)
            self.assertEqual(ocr["running_count"], 1)
            self.assertEqual(ocr["missing_worker_count"], 1)
            self.assertTrue(ocr["missing_worker"])
            self.assertEqual(ocr["capacity_remaining"], 0)
            self.assertEqual(ocr["oldest_queued_document_id"], "doc_b")

            demands = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "capability-demands",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli_metrics",
            )
            self.assertTrue(demands["ok"])
            demand_payload = demands["demands"]
            self.assertEqual(demand_payload["count"], 1)
            self.assertEqual(demand_payload["claimable_queued_count"], 1)
            self.assertEqual(demand_payload["worker_deficit_count"], 1)
            self.assertIsNone(demand_payload["demands"][0]["capability"])
            self.assertEqual(demand_payload["demands"][0]["process_ids"], ["ocr"])

            prometheus = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "metrics-prometheus",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli_metrics",
            )
            self.assertTrue(prometheus["ok"])
            self.assertIn(
                "fala_runtime_process_claimable_queued",
                prometheus["metrics"],
            )
            self.assertIn('process_id="ocr"', prometheus["metrics"])
            self.assertIn("fala_runtime_capability_worker_deficit", prometheus["metrics"])

            health = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "health",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli_metrics",
            )
            self.assertTrue(health["ok"])
            self.assertEqual(health["health"]["status"], "critical")
            self.assertGreaterEqual(health["health"]["issue_count"], 1)
            self.assertEqual(health["health"]["issues"][0]["code"], "missing_worker")

            async def record_worker() -> None:
                await RuntimeService(
                    registry=PipelineRegistry.from_directory(pipeline_dir),
                    store=SQLiteStateStore(db_path),
                ).record_worker_heartbeat(
                    run_id="run_cli_metrics",
                    worker_id="worker-ocr",
                    pipeline_id="metrics_flow",
                    adapter_kind="queue",
                    capabilities=["ocr"],
                    status=RuntimeWorkerStatus.working,
                    current_document_id="doc_a",
                    current_process_id="ocr",
                )

            asyncio.run(record_worker())
            health = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "worker-health",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli_metrics",
            )
            self.assertTrue(health["ok"])
            self.assertEqual(health["worker_count"], 1)
            self.assertEqual(health["healthy_count"], 1)
            self.assertEqual(health["workers"][0]["worker_id"], "worker-ocr")
            self.assertEqual(health["workers"][0]["current_process_id"], "ocr")

            trace = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "trace",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli_metrics",
                "--document-id",
                "doc_a",
                "--process-id",
                "ocr",
            )
            self.assertTrue(trace["ok"])
            payload = trace["trace"]
            self.assertEqual(payload["process_count"], 1)
            self.assertEqual(payload["attempt_count"], 1)
            self.assertEqual(payload["processes"][0]["attempts"][1]["attempt"], 1)
            self.assertEqual(payload["processes"][0]["attempts"][1]["worker_id"], "worker-ocr")

            chunk = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "stream-append",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli_metrics",
                "--document-id",
                "doc_a",
                "--process-id",
                "ocr",
                "--stream-id",
                "pages",
                "--kind",
                "page",
                "--value",
                "text=hello",
            )
            self.assertTrue(chunk["ok"])
            self.assertEqual(chunk["chunk"]["sequence"], 0)

            stream = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "stream-list",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli_metrics",
                "--document-id",
                "doc_a",
                "--process-id",
                "ocr",
                "--stream-id",
                "pages",
            )
            self.assertTrue(stream["ok"])
            self.assertEqual(stream["chunk_count"], 1)
            self.assertEqual(stream["chunks"][0]["values"]["text"], "hello")

            checkpoint = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "stream-checkpoint",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli_metrics",
                "--document-id",
                "doc_a",
                "--process-id",
                "ocr",
                "--stream-id",
                "pages",
                "--consumer-id",
                "export",
                "--sequence",
                "0",
                "--chunk-id",
                chunk["chunk"]["chunk_id"],
            )
            self.assertTrue(checkpoint["ok"])
            self.assertEqual(checkpoint["checkpoint"]["sequence"], 0)

            checkpoint_get = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "stream-checkpoint-get",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli_metrics",
                "--document-id",
                "doc_a",
                "--process-id",
                "ocr",
                "--stream-id",
                "pages",
                "--consumer-id",
                "export",
            )
            self.assertEqual(
                checkpoint_get["checkpoint"]["chunk_id"],
                chunk["chunk"]["chunk_id"],
            )

            next_chunk = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "stream-append",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli_metrics",
                "--document-id",
                "doc_a",
                "--process-id",
                "ocr",
                "--stream-id",
                "pages",
                "--kind",
                "page",
                "--value",
                "text=next",
            )
            self.assertEqual(next_chunk["chunk"]["sequence"], 1)

            lag = _run_cli(
                "--pipeline-dir",
                str(pipeline_dir),
                "stream-lag",
                "--db",
                str(db_path),
                "--run-id",
                "run_cli_metrics",
                "--document-id",
                "doc_a",
                "--process-id",
                "ocr",
                "--stream-id",
                "pages",
                "--consumer-id",
                "export",
            )
            self.assertTrue(lag["ok"])
            self.assertEqual(lag["stream_lag"]["count"], 1)
            self.assertEqual(lag["stream_lag"]["items"][0]["lag"], 1)
            self.assertEqual(
                lag["stream_lag"]["items"][0]["checkpoint_sequence"],
                0,
            )

    def test_runtime_web_panel_creates_batch_run(self) -> None:
        pipeline = PipelineSpec(
            id="web_batch",
            title="Web Batch",
            steps=[
                ProcessSpec(
                    id="ingest",
                    title="Ingest",
                    adapter=AdapterSpec(kind="queue", queue="web.ingest"),
                )
            ],
        )
        store = InMemoryStateStore()
        service = RuntimeService(
            registry=PipelineRegistry([pipeline]),
            store=store,
        )
        app = create_runtime_web_app(service=service)

        async def run_web() -> None:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport,
                base_url="http://testserver",
                follow_redirects=False,
            ) as client:
                form = await client.get("/runs/new")
                self.assertEqual(form.status_code, 200)
                self.assertIn("Web Batch", form.text)

                created = await client.post(
                    "/runs/new",
                    data={
                        "run_id": "run_from_web",
                        "title": "Web import",
                        "pipeline_id": "web_batch",
                        "document_type": "generic_document",
                        "media_type": "application/pdf",
                        "documents": "\n".join(
                            [
                                "doc_a=file:///tmp/doc_a.pdf",
                                "s3://bucket/email_1.eml",
                            ]
                        ),
                        "metadata": "source=web\nteam=ops",
                    },
                )
                self.assertEqual(created.status_code, 303)
                self.assertEqual(created.headers["location"], "/runs/run_from_web")

                detail = await client.get("/runs/run_from_web")
                self.assertEqual(detail.status_code, 200)
                self.assertIn("Web import", detail.text)
                self.assertIn("/process-runtime/documents", detail.text)
                self.assertIn("/process-runtime/processes", detail.text)
                self.assertIn("/process-runtime/capability-demands", detail.text)

                documents = await client.get(
                    "/runs/run_from_web/process-runtime/documents",
                    params={"status": "queued", "limit": "1"},
                )
                self.assertEqual(documents.status_code, 200)
                self.assertIn("Document registry", documents.text)
                self.assertIn("doc_a", documents.text)
                self.assertIn("status=queued", documents.text)
                self.assertIn("Next", documents.text)

                processes = await client.get(
                    "/runs/run_from_web/process-runtime/processes",
                    params={
                        "status": "queued",
                        "process_id": "ingest",
                        "adapter_kind": "queue",
                        "resource_pool": "default",
                        "limit": "1",
                    },
                )
                self.assertEqual(processes.status_code, 200)
                self.assertIn("Process registry", processes.text)
                self.assertIn("ingest", processes.text)
                self.assertIn("process_id=ingest", processes.text)
                self.assertIn("adapter_kind=queue", processes.text)
                self.assertIn("resource_pool=default", processes.text)
                self.assertIn("Next", processes.text)

                capability_demands = await client.get(
                    "/runs/run_from_web/process-runtime/capability-demands"
                )
                self.assertEqual(capability_demands.status_code, 200)
                self.assertIn("Capability demand", capability_demands.text)
                self.assertIn("unknown", capability_demands.text)
                self.assertIn("2 claimable", capability_demands.text)

                partial = await client.get("/runs/run_from_web/process-runtime")
                self.assertEqual(partial.status_code, 200)
                self.assertIn("doc_a", partial.text)
                self.assertIn("email_1.eml", partial.text)

        asyncio.run(run_web())

        doc_a = asyncio.run(
            store.get_document(run_id="run_from_web", document_id="doc_a")
        )
        email = asyncio.run(
            store.get_document(run_id="run_from_web", document_id="email_1.eml")
        )
        self.assertIsNotNone(doc_a)
        self.assertIsNotNone(email)
        assert doc_a is not None
        assert email is not None
        self.assertEqual(doc_a.source_uri, "file:///tmp/doc_a.pdf")
        self.assertEqual(doc_a.document_type, "generic_document")
        self.assertEqual(doc_a.media_type, "application/pdf")
        self.assertEqual(doc_a.metadata["source"], "web")
        self.assertEqual(doc_a.status, RuntimeDocumentStatus.queued)
        self.assertEqual(email.source_uri, "s3://bucket/email_1.eml")
        self.assertEqual(email.metadata["team"], "ops")

    def test_runtime_web_panel_auto_routes_batch_run(self) -> None:
        registry = PipelineRegistry()
        registry.add_package(
            WorkflowPackageSpec(
                id="web_mixed_pkg",
                pipelines=["invoice.yaml", "email.yaml"],
                document_types=[
                    DocumentTypeSpec(
                        id="invoice_document",
                        media_types=["application/pdf"],
                        extensions=[".pdf"],
                    ),
                    DocumentTypeSpec(
                        id="email_document",
                        media_types=["message/rfc822"],
                        extensions=[".eml"],
                    ),
                ],
                artifact_kinds=[
                    ArtifactKindSpec(id="invoice_source"),
                    ArtifactKindSpec(id="email_source"),
                ],
                capabilities=[
                    CapabilitySpec(
                        id="ingest_invoice",
                        accepts_document_types=["invoice_document"],
                        emits_artifact_kinds=["invoice_source"],
                    ),
                    CapabilitySpec(
                        id="ingest_email",
                        accepts_document_types=["email_document"],
                        emits_artifact_kinds=["email_source"],
                    ),
                ],
            )
        )
        registry.add(
            PipelineSpec(
                id="invoice_flow",
                title="Invoice Flow",
                steps=[
                    ProcessSpec(
                        id="ingest_invoice",
                        capability="ingest_invoice",
                        adapter=AdapterSpec(kind="queue", queue="invoice.ingest"),
                    )
                ],
            ),
            package_id="web_mixed_pkg",
        )
        registry.add(
            PipelineSpec(
                id="email_flow",
                title="Email Flow",
                steps=[
                    ProcessSpec(
                        id="ingest_email",
                        capability="ingest_email",
                        adapter=AdapterSpec(kind="queue", queue="email.ingest"),
                    )
                ],
            ),
            package_id="web_mixed_pkg",
        )
        registry.validate_package_workers("web_mixed_pkg")
        store = InMemoryStateStore()
        service = RuntimeService(registry=registry, store=store)
        app = create_runtime_web_app(service=service)

        async def run_web() -> None:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport,
                base_url="http://testserver",
                follow_redirects=False,
            ) as client:
                form = await client.get("/runs/new")
                self.assertEqual(form.status_code, 200)
                self.assertIn("Auto route", form.text)
                self.assertNotIn("name=\"pipeline_id\" required", form.text)

                created = await client.post(
                    "/runs/new",
                    data={
                        "run_id": "run_web_auto",
                        "title": "Web auto route",
                        "auto_route": "1",
                        "documents": "\n".join(
                            [
                                "invoice=file:///tmp/invoice.pdf",
                                "mail=file:///tmp/mail.eml",
                            ]
                        ),
                    },
                )
                self.assertEqual(created.status_code, 303, created.text)
                self.assertEqual(created.headers["location"], "/runs/run_web_auto")

                detail = await client.get("/runs/run_web_auto")
                self.assertEqual(detail.status_code, 200)
                self.assertIn("/process-runtime/provenance", detail.text)

                provenance = await client.get(
                    "/runs/run_web_auto/process-runtime/provenance"
                )
                self.assertEqual(provenance.status_code, 200)
                self.assertIn("Run provenance", provenance.text)
                self.assertIn("Route report", provenance.text)
                self.assertIn("auto:invoice_flow:invoice_document", provenance.text)
                self.assertIn("auto:email_flow:email_document", provenance.text)
                self.assertIn("2 / 4 candidates", provenance.text)
                self.assertIn("1 / 2 matched", provenance.text)
                self.assertIn("rejected", provenance.text)
                self.assertIn("extension_mismatch", provenance.text)
                self.assertIn("media_type_missing", provenance.text)

        asyncio.run(run_web())

        invoice = asyncio.run(
            store.get_document(run_id="run_web_auto", document_id="invoice")
        )
        email = asyncio.run(
            store.get_document(run_id="run_web_auto", document_id="mail")
        )
        self.assertIsNotNone(invoice)
        self.assertIsNotNone(email)
        assert invoice is not None
        assert email is not None
        self.assertEqual(invoice.pipeline_id, "invoice_flow")
        self.assertEqual(invoice.document_type, "invoice_document")
        self.assertEqual(email.pipeline_id, "email_flow")
        self.assertEqual(email.document_type, "email_document")
        run = asyncio.run(store.get_run("run_web_auto"))
        self.assertIsNotNone(run)
        assert run is not None
        provenance = run.metadata["process_runtime"]["run_provenance"]
        self.assertEqual(provenance["route_report"]["routed_count"], 2)
        self.assertEqual(len(provenance["route_report_sha256"]), 64)

    def test_runtime_web_panel_uploads_files_into_artifact_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipeline = PipelineSpec(
                id="web_upload",
                title="Web Upload",
                steps=[
                    ProcessSpec(
                        id="ingest",
                        title="Ingest",
                        adapter=AdapterSpec(kind="queue", queue="web.upload"),
                    )
                ],
            )
            store = InMemoryStateStore()
            service = RuntimeService(
                registry=PipelineRegistry([pipeline]),
                store=store,
                artifact_store_root=root / "artifact-store",
            )
            app = create_runtime_web_app(service=service)

            async def run_web() -> None:
                transport = httpx.ASGITransport(app=app)
                async with httpx.AsyncClient(
                    transport=transport,
                    base_url="http://testserver",
                    follow_redirects=False,
                ) as client:
                    created = await client.post(
                        "/runs/new",
                        data={
                            "run_id": "run_upload",
                            "title": "Uploaded docs",
                            "pipeline_id": "web_upload",
                            "document_type": "generic_document",
                            "metadata": "source=upload",
                        },
                        files=[
                            ("files", ("upload.txt", b"hello upload", "text/plain")),
                        ],
                    )
                    self.assertEqual(created.status_code, 303)
                    self.assertEqual(created.headers["location"], "/runs/run_upload")

            asyncio.run(run_web())

            document = asyncio.run(
                store.get_document(run_id="run_upload", document_id="upload.txt")
            )
            self.assertIsNotNone(document)
            assert document is not None
            self.assertEqual(document.title, "upload.txt")
            self.assertEqual(document.media_type, "text/plain")
            self.assertEqual(document.metadata["source"], "upload")
            self.assertEqual(document.metadata["uploaded"], True)
            self.assertTrue(document.source_uri)
            assert document.source_uri is not None
            self.assertTrue(document.source_uri.startswith("fala-artifact://sha256/"))
            resolved = service.artifact_store.resolve(
                ArtifactRef(kind="generic_document", uri=document.source_uri)
            )
            self.assertEqual(resolved.read_bytes(), b"hello upload")

    def test_runtime_web_panel_lists_scaffold_blueprints(self) -> None:
        service = RuntimeService(
            registry=PipelineRegistry([]),
            store=InMemoryStateStore(),
        )
        app = create_runtime_web_app(service=service)

        async def fetch() -> None:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport,
                base_url="http://testserver",
            ) as client:
                catalog = await client.get("/process-runtime/blueprints")
                self.assertEqual(catalog.status_code, 200)
                self.assertIn("document_digitalization", catalog.text)
                self.assertIn("generative_media", catalog.text)
                self.assertIn("uv run fala scaffold --blueprint", catalog.text)

                filtered = await client.get(
                    "/process-runtime/blueprints",
                    params={"query": "translated_document"},
                )
                self.assertEqual(filtered.status_code, 200)
                self.assertIn("document_translation_review", filtered.text)
                self.assertIn("1 matches", filtered.text)
                self.assertNotIn("document_redaction_review", filtered.text)

                detail = await client.get(
                    "/process-runtime/blueprints",
                    params={"blueprint": "llm_document_processing"},
                )
                self.assertEqual(detail.status_code, 200)
                self.assertIn("LLM document processing", detail.text)
                self.assertIn("generate_response", detail.text)
                self.assertIn("tokens", detail.text)
                self.assertIn("manual", detail.text)
                self.assertIn("llm_pool", detail.text)

                missing = await client.get(
                    "/process-runtime/blueprints",
                    params={"blueprint": "not_real"},
                )
                self.assertEqual(missing.status_code, 404)

        asyncio.run(fetch())

    def test_runtime_web_panel_shows_package_readiness(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package_dir = root / "pkg"
            package_dir.mkdir()
            (package_dir / "process-runtime-package.yaml").write_text(
                textwrap.dedent(
                    """
                    package: web_readiness
                    document_types:
                      - id: generic_document
                      - id: email_document
                    artifact_kinds:
                      - id: extracted_payload
                    capabilities:
                      - id: extract_document
                        accepts_document_types: [generic_document]
                        emits_artifact_kinds: [extracted_payload]
                    pipelines:
                      - extract.yaml
                    """
                ).strip(),
                encoding="utf-8",
            )
            (package_dir / "extract.yaml").write_text(
                textwrap.dedent(
                    """
                    pipeline: extract_flow
                    steps:
                      - id: extract
                        capability: extract_document
                        adapter:
                          kind: queue
                          queue: web.extract
                    """
                ).strip(),
                encoding="utf-8",
            )
            service = RuntimeService(
                registry=PipelineRegistry.from_directory(root),
                store=InMemoryStateStore(),
            )
            app = create_runtime_web_app(service=service)

            async def fetch() -> None:
                transport = httpx.ASGITransport(app=app)
                async with httpx.AsyncClient(
                    transport=transport,
                    base_url="http://testserver",
                ) as client:
                    response = await client.get("/process-runtime/pipelines")
                    self.assertEqual(response.status_code, 200)
                    self.assertIn("Readiness", response.text)
                    self.assertIn("readiness ok", response.text)
                    self.assertIn("3 warnings", response.text)
                    self.assertIn("1 / 2 routeable document types", response.text)
                    self.assertIn(
                        "0 / 1 queue processes covered by package workers",
                        response.text,
                    )
                    self.assertIn("queue_step_without_package_worker", response.text)
                    self.assertIn("document_type_not_routeable", response.text)
                    self.assertIn("sample_run_input_missing", response.text)
                    self.assertIn(
                        "/api/process-runtime/packages/web_readiness/readiness",
                        response.text,
                    )

            asyncio.run(fetch())

    def test_runtime_web_panel_shows_project_readiness(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            project_dir = Path(tmp) / "document_workspace"
            _run_cli(
                "init-project",
                "--output-dir",
                str(project_dir),
                "--project-id",
                "document_workspace",
                "--blueprint",
                "document_digitalization",
                "--blueprint",
                "email_processing",
                "--adapter-kind",
                "queue",
            )
            service = RuntimeService(
                registry=PipelineRegistry.from_directory(project_dir / "pipelines"),
                store=SQLiteStateStore(project_dir / "runtime.db"),
            )
            app = create_runtime_web_app(service=service, project_dir=project_dir)

            async def fetch() -> None:
                transport = httpx.ASGITransport(app=app)
                async with httpx.AsyncClient(
                    transport=transport,
                    base_url="http://testserver",
                ) as client:
                    api = await client.get("/api/process-runtime/project")
                    self.assertEqual(api.status_code, 200)
                    payload = api.json()
                    self.assertTrue(payload["configured"])
                    self.assertTrue(payload["ok"])
                    self.assertEqual(
                        payload["project"]["project_id"],
                        "document_workspace",
                    )
                    self.assertEqual(
                        payload["project"]["mixed_run_input"]["pipeline_counts"],
                        {
                            "document_digitalization_flow": 2,
                            "email_processing_flow": 2,
                        },
                    )
                    runtime_client = ProcessRuntimeClient(
                        "http://testserver",
                        client=client,
                    )
                    project = await runtime_client.get_project()
                    self.assertTrue(project["configured"])
                    self.assertEqual(
                        project["project"]["project_id"],
                        "document_workspace",
                    )
                    self.assertEqual(project["project"]["alerts"]["rule_count"], 4)
                    self.assertTrue(
                        project["project"]["lifecycle"]["run_retention"]["enabled"]
                    )
                    spec = await runtime_client.get_project_spec(
                        base_url="http://testserver",
                        run_id="run_project_spec",
                    )
                    self.assertEqual(spec["project_id"], "document_workspace")
                    self.assertEqual(spec["package_index"]["package_count"], 2)
                    self.assertEqual(spec["intake"]["document_count"], 4)
                    self.assertEqual(spec["routes"]["count"], 4)
                    self.assertGreater(spec["worker_commands"]["worker_count"], 0)
                    self.assertEqual(spec["worker_commands"]["run_id"], "run_project_spec")
                    spec_api = await client.get(
                        "/api/process-runtime/project/spec",
                        params={
                            "base_url": "http://testserver",
                            "run_id": "run_project_spec",
                        },
                    )
                    self.assertEqual(spec_api.status_code, 200)
                    self.assertEqual(
                        spec_api.json()["spec"]["worker_commands"]["run_id"],
                        "run_project_spec",
                    )
                    bootstrap = await runtime_client.get_project_bootstrap(
                        base_url="http://testserver",
                        run_id="run_project_bootstrap",
                    )
                    self.assertTrue(bootstrap["configured"])
                    self.assertTrue(bootstrap["ok"])
                    self.assertTrue(bootstrap["db_configured"])
                    self.assertTrue(bootstrap["db"]["schema"]["ok"])
                    self.assertEqual(
                        bootstrap["db"]["schema"]["current_version"],
                        RUNTIME_SCHEMA_VERSION,
                    )
                    self.assertEqual(
                        bootstrap["check"]["run_id"],
                        "run_project_bootstrap",
                    )
                    self.assertIn(
                        "project_smoke",
                        {item["id"] for item in bootstrap["commands"]},
                    )
                    bootstrap_api = await client.get(
                        "/api/process-runtime/project/bootstrap",
                        params={
                            "base_url": "http://testserver",
                            "run_id": "run_project_bootstrap",
                        },
                    )
                    self.assertEqual(bootstrap_api.status_code, 200)
                    self.assertEqual(
                        bootstrap_api.json()["db"]["schema"]["current_version"],
                        RUNTIME_SCHEMA_VERSION,
                    )
                    bootstrap_page = await client.get(
                        "/project/bootstrap",
                        params={
                            "base_url": "http://testserver",
                            "run_id": "run_project_bootstrap",
                        },
                    )
                    self.assertEqual(bootstrap_page.status_code, 200)
                    self.assertIn("Project bootstrap", bootstrap_page.text)
                    self.assertIn("Runtime database", bootstrap_page.text)
                    self.assertIn("Operator commands", bootstrap_page.text)
                    self.assertIn("project-smoke", bootstrap_page.text)
                    self.assertIn(
                        "/api/process-runtime/project/bootstrap",
                        bootstrap_page.text,
                    )
                    created = await runtime_client.create_project_run(
                        run_id="run_project_api",
                        metadata={"operator": "api"},
                    )
                    self.assertTrue(created["ok"])
                    self.assertEqual(created["document_count"], 4)
                    self.assertEqual(created["route_report"]["routed_count"], 4)
                    api_run = await service.get_run("run_project_api")
                    self.assertIsNotNone(api_run)
                    assert api_run is not None
                    self.assertEqual(api_run.metadata["project_id"], "document_workspace")
                    self.assertEqual(api_run.metadata["operator"], "api")
                    self.assertEqual(
                        api_run.metadata["process_runtime"]["project"]["project_id"],
                        "document_workspace",
                    )

                    page = await client.get("/project")
                    self.assertEqual(page.status_code, 200)
                    self.assertIn("document_workspace", page.text)
                    self.assertIn("document_digitalization_flow", page.text)
                    self.assertIn("email_processing_flow", page.text)
                    self.assertIn("Workspace files", page.text)
                    self.assertIn("/api/process-runtime/project", page.text)
                    self.assertIn('href="/project/spec"', page.text)
                    self.assertIn("run_project_api", page.text)
                    self.assertIn('action="/project/runs"', page.text)
                    spec_page = await client.get(
                        "/project/spec",
                        params={
                            "base_url": "http://testserver",
                            "run_id": "run_project_spec",
                        },
                    )
                    self.assertEqual(spec_page.status_code, 200)
                    self.assertIn("Project spec", spec_page.text)
                    self.assertIn("bootstrap runbook", spec_page.text)
                    self.assertIn("Worker commands", spec_page.text)
                    self.assertIn("run_project_spec", spec_page.text)
                    self.assertIn(
                        "/api/process-runtime/project/spec",
                        spec_page.text,
                    )

                    created_from_web = await client.post(
                        "/project/runs",
                        data={
                            "run_id": "run_project_web",
                            "title": "Project web run",
                            "existing_run_policy": "error",
                            "existing_document_policy": "error",
                        },
                        follow_redirects=False,
                    )
                    self.assertEqual(created_from_web.status_code, 303)
                    self.assertEqual(
                        created_from_web.headers["location"],
                        "/runs/run_project_web",
                    )
                    web_run = await service.get_run("run_project_web")
                    self.assertIsNotNone(web_run)
                    assert web_run is not None
                    self.assertEqual(web_run.title, "Project web run")
                    self.assertEqual(web_run.metadata["project_id"], "document_workspace")
                    history = await runtime_client.get_project_runs()
                    self.assertEqual(history["run_count"], 2)
                    self.assertEqual(history["document_count"], 8)
                    self.assertEqual(
                        history["package_counts"],
                        {
                            "document_digitalization": 4,
                            "email_processing": 4,
                        },
                    )
                    self.assertEqual(
                        history["document_type_counts"],
                        {
                            "email_attachment_document": 2,
                            "email_document": 2,
                            "generic_document": 2,
                            "page_document": 2,
                        },
                    )
                    filtered_history = await runtime_client.get_project_runs(
                        package_id="email_processing",
                    )
                    self.assertEqual(filtered_history["run_count"], 2)
                    self.assertEqual(filtered_history["document_count"], 4)
                    self.assertEqual(
                        filtered_history["pipeline_counts"],
                        {"email_processing_flow": 4},
                    )
                    filtered_page = await client.get(
                        "/project",
                        params={"package_id": "email_processing"},
                    )
                    self.assertEqual(filtered_page.status_code, 200)
                    self.assertIn("email_processing_flow: 2", filtered_page.text)
                    self.assertIn("Project runs", filtered_page.text)

                    email_pipeline = service.registry.get("email_processing_flow")
                    email_process_id = email_pipeline.steps[0].id
                    await PipelineScheduler(
                        email_pipeline,
                        service.store,
                    ).record_process_failure(
                        run_id="run_project_api",
                        document_id="email_processing_sample.eml",
                        process_id=email_process_id,
                        reason="mail parse failed",
                        error_kind="llm_parse_error",
                    )
                    document_pipeline = service.registry.get(
                        "document_digitalization_flow"
                    )
                    document_process_id = document_pipeline.steps[0].id
                    await service.append_stream_chunk(
                        run_id="run_project_api",
                        document_id="document_digitalization_sample.pdf",
                        process_id=document_process_id,
                        stream_id="pages",
                        kind="page",
                        values={"text": "page one"},
                    )
                    await service.put_stream_checkpoint(
                        run_id="run_project_api",
                        document_id="document_digitalization_sample.pdf",
                        process_id=document_process_id,
                        stream_id="pages",
                        consumer_id="ocr_index",
                        sequence=-1,
                    )
                    supervision = await runtime_client.get_project_supervision(
                        queued_after_seconds=0,
                        min_lag=1,
                    )
                    self.assertEqual(supervision["run_count"], 2)
                    self.assertEqual(supervision["dead_letter_count"], 1)
                    self.assertGreaterEqual(supervision["stuck_work_count"], 1)
                    self.assertGreaterEqual(supervision["stream_lag_count"], 1)
                    self.assertEqual(
                        supervision["dead_letter"]["items"][0]["package_id"],
                        "email_processing",
                    )
                    self.assertEqual(
                        supervision["dead_letter"]["items"][0]["operation_type"],
                        "ingest",
                    )
                    self.assertEqual(
                        supervision["dead_letter"]["items"][0]["reason"],
                        "mail parse failed",
                    )
                    self.assertIn("ingest", supervision["operation_type_counts"])
                    self.assertIn(
                        "document_digitalization",
                        supervision["package_counts"],
                    )
                    filtered_supervision = await runtime_client.get_project_supervision(
                        package_id="email_processing",
                        operation_type="ingest",
                        queued_after_seconds=0,
                    )
                    self.assertEqual(filtered_supervision["dead_letter_count"], 1)
                    self.assertEqual(filtered_supervision["stream_lag_count"], 0)
                    supervision_api = await client.get(
                        "/api/process-runtime/project/supervision",
                        params={
                            "package_id": "email_processing",
                            "operation_type": "ingest",
                            "queued_after_seconds": "0",
                        },
                    )
                    self.assertEqual(supervision_api.status_code, 200)
                    self.assertEqual(
                        supervision_api.json()["supervision"]["dead_letter_count"],
                        1,
                    )
                    supervision_page = await client.get(
                        "/project/supervision",
                        params={
                            "package_id": "email_processing",
                            "operation_type": "ingest",
                            "queued_after_seconds": "0",
                        },
                    )
                    self.assertEqual(supervision_page.status_code, 200)
                    self.assertIn("Project supervision", supervision_page.text)
                    self.assertIn("mail parse failed", supervision_page.text)
                    self.assertIn("email_processing", supervision_page.text)
                    self.assertIn(
                        "/api/process-runtime/project/supervision",
                        supervision_page.text,
                    )
                    operations = await runtime_client.get_project_operations(
                        package_id="email_processing",
                        operation_type="ingest",
                        queued_after_seconds=0,
                    )
                    self.assertEqual(operations["status"], "critical")
                    self.assertEqual(
                        operations["supervision"]["dead_letter_count"],
                        1,
                    )
                    self.assertGreaterEqual(
                        operations["queue"]["worker_deficit_count"],
                        1,
                    )
                    self.assertGreaterEqual(
                        operations["capability_demands"]["count"],
                        1,
                    )
                    self.assertIn("missing_worker", operations["issue_code_counts"])
                    operations_api = await client.get(
                        "/api/process-runtime/project/operations",
                        params={
                            "package_id": "email_processing",
                            "operation_type": "ingest",
                            "queued_after_seconds": "0",
                        },
                    )
                    self.assertEqual(operations_api.status_code, 200)
                    self.assertEqual(
                        operations_api.json()["operations"]["status"],
                        "critical",
                    )
                    operations_page = await client.get(
                        "/project/operations",
                        params={
                            "package_id": "email_processing",
                            "operation_type": "ingest",
                            "queued_after_seconds": "0",
                        },
                    )
                    self.assertEqual(operations_page.status_code, 200)
                    self.assertIn("Project operations", operations_page.text)
                    self.assertIn("Capability demand", operations_page.text)
                    self.assertIn("Health issues", operations_page.text)
                    self.assertIn("missing_worker", operations_page.text)
                    self.assertIn(
                        "/api/process-runtime/project/operations",
                        operations_page.text,
                    )
                    alerts = await runtime_client.get_project_alerts(
                        package_id="email_processing",
                        operation_type="ingest",
                        queued_after_seconds=0,
                    )
                    self.assertEqual(alerts["status"], "critical")
                    self.assertGreaterEqual(alerts["firing_count"], 2)
                    self.assertIn(
                        "worker_deficit_present",
                        {item["rule_id"] for item in alerts["alerts"]},
                    )
                    self.assertIn(
                        "dead_letter_present",
                        {item["rule_id"] for item in alerts["alerts"]},
                    )
                    alerts_api = await client.get(
                        "/api/process-runtime/project/alerts",
                        params={
                            "package_id": "email_processing",
                            "operation_type": "ingest",
                            "queued_after_seconds": "0",
                        },
                    )
                    self.assertEqual(alerts_api.status_code, 200)
                    self.assertEqual(
                        alerts_api.json()["alerts"]["status"],
                        "critical",
                    )
                    alerts_page = await client.get(
                        "/project/alerts",
                        params={
                            "package_id": "email_processing",
                            "operation_type": "ingest",
                            "queued_after_seconds": "0",
                        },
                    )
                    self.assertEqual(alerts_page.status_code, 200)
                    self.assertIn("Project alerts", alerts_page.text)
                    self.assertIn("Firing alerts", alerts_page.text)
                    self.assertIn("worker_deficit_present", alerts_page.text)
                    self.assertIn("dead_letter_present", alerts_page.text)
                    self.assertIn(
                        "/api/process-runtime/project/alerts",
                        alerts_page.text,
                    )
                    lifecycle_statuses = [
                        "created",
                        "paused",
                        "queued",
                        "running",
                        "completed",
                        "failed",
                        "cancelled",
                    ]
                    lifecycle = await runtime_client.get_project_lifecycle(
                        before="2999-01-01T00:00:00+00:00",
                        statuses=lifecycle_statuses,
                    )
                    self.assertTrue(lifecycle["dry_run"])
                    self.assertEqual(lifecycle["candidate_count"], 2)
                    self.assertEqual(lifecycle["deleted_run_count"], 0)
                    self.assertEqual(
                        lifecycle["retention"]["statuses"],
                        lifecycle_statuses,
                    )
                    self.assertIn("artifact_gc", lifecycle)
                    lifecycle_post = await runtime_client.run_project_lifecycle(
                        before="2999-01-01T00:00:00+00:00",
                        statuses=lifecycle_statuses,
                        delete=False,
                    )
                    self.assertTrue(lifecycle_post["dry_run"])
                    self.assertEqual(lifecycle_post["candidate_count"], 2)
                    lifecycle_api = await client.get(
                        "/api/process-runtime/project/lifecycle",
                        params=[
                            ("before", "2999-01-01T00:00:00+00:00"),
                            *[
                                ("status", item)
                                for item in lifecycle_statuses
                            ],
                        ],
                    )
                    self.assertEqual(lifecycle_api.status_code, 200)
                    self.assertEqual(
                        lifecycle_api.json()["lifecycle"]["candidate_count"],
                        2,
                    )
                    lifecycle_page = await client.get(
                        "/project/lifecycle",
                        params=[
                            ("before", "2999-01-01T00:00:00+00:00"),
                            *[
                                ("status", item)
                                for item in lifecycle_statuses
                            ],
                        ],
                    )
                    self.assertEqual(lifecycle_page.status_code, 200)
                    self.assertIn("Project lifecycle", lifecycle_page.text)
                    self.assertIn("Run retention", lifecycle_page.text)
                    self.assertIn("Artifact GC", lifecycle_page.text)
                    self.assertIn(
                        "/api/process-runtime/project/lifecycle",
                        lifecycle_page.text,
                    )

                    runs = await client.get("/runs")
                    self.assertEqual(runs.status_code, 200)
                    self.assertIn('href="/project"', runs.text)

            asyncio.run(fetch())

    def test_runtime_web_panel_lists_run_and_events(self) -> None:
        pipeline = PipelineSpec(
            id="web_test",
            steps=[
                ProcessSpec(
                    id="extract",
                    title="Extract",
                    adapter=AdapterSpec(kind="queue", queue="web.extract"),
                )
            ],
        )
        store = InMemoryStateStore()
        service = RuntimeService(
            registry=PipelineRegistry([pipeline]),
            store=store,
        )

        async def seed() -> None:
            await service.initialize_document(
                run_id="run_web",
                document_id="doc_web.pdf",
                pipeline_id="web_test",
                values={"source": "doc_web.pdf"},
            )
            await store.append_event(
                ProcessEvent(
                    run_id="run_web",
                    document_id="doc_web.pdf",
                    process_id="extract",
                    type="process.progress",
                    status=ProcessStatus.running,
                    data={"percent": 25},
                )
            )
            await service.append_stream_chunk(
                run_id="run_web",
                document_id="doc_web.pdf",
                process_id="extract",
                stream_id="pages",
                kind="page",
                values={"text": "page text"},
            )
            await service.put_stream_checkpoint(
                run_id="run_web",
                document_id="doc_web.pdf",
                process_id="extract",
                stream_id="pages",
                consumer_id="review",
                sequence=-1,
            )

        asyncio.run(seed())

        app = create_runtime_web_app(service=service)

        async def fetch() -> None:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport,
                base_url="http://testserver",
            ) as client:
                runs_response = await client.get("/runs")
                self.assertEqual(runs_response.status_code, 200)
                self.assertIn("run_web", runs_response.text)
                self.assertIn("web_test", runs_response.text)

                detail_response = await client.get("/runs/run_web")
                self.assertEqual(detail_response.status_code, 200)
                self.assertIn("run_web", detail_response.text)
                self.assertIn("/process-runtime/events", detail_response.text)

                partial_response = await client.get("/runs/run_web/process-runtime")
                self.assertEqual(partial_response.status_code, 200)
                self.assertIn("Extract", partial_response.text)
                self.assertIn("Capacity", partial_response.text)
                self.assertIn("Trace", partial_response.text)
                self.assertIn("Streamy", partial_response.text)
                self.assertIn("1 stream chunks", partial_response.text)
                self.assertIn("consumers: review", partial_response.text)
                self.assertIn("process.progress", partial_response.text)
                self.assertIn("/actions/retry?reason=web%20panel", partial_response.text)
                self.assertIn("/actions/skip?reason=web%20panel", partial_response.text)

                lag_response = await client.get(
                    "/runs/run_web/process-runtime/stream-lag"
                )
                self.assertEqual(lag_response.status_code, 200)
                self.assertIn("Stream lag", lag_response.text)
                self.assertIn("1 lagging", lag_response.text)
                self.assertIn("review", lag_response.text)
                self.assertIn("seq -1", lag_response.text)

                timeline_response = await client.get(
                    "/runs/run_web/process-runtime/events"
                )
                self.assertEqual(timeline_response.status_code, 200)
                self.assertIn("Run event timeline", timeline_response.text)
                self.assertIn("process.progress", timeline_response.text)
                self.assertIn("doc_web.pdf", timeline_response.text)
                self.assertIn(
                    "/api/runs/run_web/process-runtime/events/stream",
                    timeline_response.text,
                )

                action_response = await client.post(
                    "/runs/run_web/process-runtime/doc_web.pdf/processes/extract/actions/skip",
                    params={"reason": "operator skip"},
                )
                self.assertEqual(action_response.status_code, 200)
                self.assertIn("skipped", action_response.text)
                self.assertIn("process.skip_requested", action_response.text)

                filtered_timeline = await client.get(
                    "/runs/run_web/process-runtime/events",
                    params={"process_id": "extract"},
                )
                self.assertEqual(filtered_timeline.status_code, 200)
                self.assertIn("extract", filtered_timeline.text)
                self.assertIn("operator skip", filtered_timeline.text)

                event_response = await client.get(
                    "/runs/run_web/process-runtime/doc_web.pdf/events?process_id=extract"
                )
                self.assertEqual(event_response.status_code, 200)
                self.assertIn("operator skip", event_response.text)

        asyncio.run(fetch())

    def test_runtime_web_panel_shows_dead_letter_and_stuck_work_controls(self) -> None:
        pipeline = PipelineSpec(
            id="web_supervision",
            steps=[
                ProcessSpec(
                    id="ingest",
                    title="Ingest",
                    capability="ingest_document",
                    adapter=AdapterSpec(kind="queue", queue="web.supervision"),
                    retry=RetryPolicy(max_attempts=1),
                    sla={"queued_after_seconds": 0},
                )
            ],
        )
        store = InMemoryStateStore()
        service = RuntimeService(
            registry=PipelineRegistry([pipeline]),
            store=store,
        )

        async def seed() -> None:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_web_supervision",
                    pipeline_id="web_supervision",
                    documents=[
                        RuntimeDocumentInput(
                            document_id="doc_failed.pdf",
                            title="Failed doc",
                        ),
                        RuntimeDocumentInput(
                            document_id="doc_stuck.pdf",
                            title="Stuck doc",
                        ),
                    ],
                )
            )
            await PipelineScheduler(pipeline, store).record_process_failure(
                run_id="run_web_supervision",
                document_id="doc_failed.pdf",
                process_id="ingest",
                reason="bad parse",
                error_kind="validation_error",
            )

        asyncio.run(seed())

        app = create_runtime_web_app(service=service)

        async def fetch() -> None:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport,
                base_url="http://testserver",
            ) as client:
                detail_response = await client.get("/runs/run_web_supervision")
                self.assertEqual(detail_response.status_code, 200)
                self.assertIn("/process-runtime/dead-letter", detail_response.text)
                self.assertIn("/process-runtime/stuck-work", detail_response.text)

                dead_letter = await client.get(
                    "/runs/run_web_supervision/process-runtime/dead-letter"
                )
                self.assertEqual(dead_letter.status_code, 200)
                self.assertIn("Dead-letter queue", dead_letter.text)
                self.assertIn("doc_failed.pdf", dead_letter.text)
                self.assertIn("bad parse", dead_letter.text)
                self.assertIn("validation_error", dead_letter.text)
                self.assertIn("Replay", dead_letter.text)

                stuck_work = await client.get(
                    "/runs/run_web_supervision/process-runtime/stuck-work",
                    params={
                        "status": ProcessStatus.queued.value,
                        "queued_after_seconds": "999",
                    },
                )
                self.assertEqual(stuck_work.status_code, 200)
                self.assertIn("Stuck work", stuck_work.text)
                self.assertIn("doc_stuck.pdf", stuck_work.text)
                self.assertIn("queued_too_long", stuck_work.text)
                self.assertIn("threshold 0.0s", stuck_work.text)
                self.assertIn("actions/retry", stuck_work.text)
                self.assertIn("actions/cancel", stuck_work.text)

                cancelled = await client.post(
                    "/runs/run_web_supervision/process-runtime/stuck-work/"
                    "doc_stuck.pdf/processes/ingest/actions/cancel",
                    data={"reason": "operator cancelled stuck work"},
                )
                self.assertEqual(cancelled.status_code, 200)
                self.assertIn(
                    "cancel requested for doc_stuck.pdf / ingest",
                    cancelled.text,
                )
                self.assertNotIn("doc_stuck.pdf</div>", cancelled.text)

                replayed = await client.post(
                    "/runs/run_web_supervision/process-runtime/dead-letter/"
                    "doc_failed.pdf/processes/ingest/replay",
                    data={"reason": "fixed source"},
                )
                self.assertEqual(replayed.status_code, 200)
                self.assertIn("Replayed doc_failed.pdf / ingest", replayed.text)
                self.assertIn(
                    "No failed process instances match these filters.",
                    replayed.text,
                )

                audit_response = await client.get(
                    "/runs/run_web_supervision/process-runtime/audit"
                )
                self.assertEqual(audit_response.status_code, 200)
                self.assertIn("process.dead_letter.replay", audit_response.text)
                self.assertIn("process.stuck_work.cancel", audit_response.text)

        asyncio.run(fetch())

    def test_runtime_web_panel_shows_results_reductions_lineage_and_manual_queue(self) -> None:
        pipeline = PipelineSpec(
            id="web_operator",
            steps=[
                ProcessSpec(
                    id="review",
                    title="Review",
                    adapter=AdapterSpec(kind="manual"),
                ),
                ProcessSpec(
                    id="export",
                    title="Export",
                    needs=["review"],
                    adapter=AdapterSpec(kind="queue", queue="web.export"),
                ),
            ],
            reduces=[
                RunReduceSpec(
                    id="review_decisions",
                    process_id="review",
                    value_key="approved",
                )
            ],
        )
        service = RuntimeService(
            registry=PipelineRegistry([pipeline]),
            store=InMemoryStateStore(),
        )

        async def seed() -> None:
            await service.create_run_with_documents(
                RuntimeRunInput(
                    run_id="run_web_operator",
                    pipeline_id="web_operator",
                    documents=[
                        RuntimeDocumentInput(
                            document_id="doc_operator.pdf",
                            title="Operator doc",
                            document_type="pdf",
                        )
                    ],
                )
            )

        asyncio.run(seed())

        app = create_runtime_web_app(service=service)

        async def fetch() -> None:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport,
                base_url="http://testserver",
            ) as client:
                detail_response = await client.get("/runs/run_web_operator")
                self.assertEqual(detail_response.status_code, 200)
                self.assertIn("/process-runtime/audit", detail_response.text)

                manual_response = await client.get(
                    "/runs/run_web_operator/process-runtime/manual"
                )
                self.assertEqual(manual_response.status_code, 200)
                self.assertIn("Manual queue", manual_response.text)
                self.assertIn("Review", manual_response.text)
                self.assertIn("waiting", manual_response.text)
                self.assertIn("manual-complete", manual_response.text)

                complete_response = await client.post(
                    "/runs/run_web_operator/process-runtime/doc_operator.pdf/processes/review/manual-complete",
                    data={
                        "values": json.dumps({"approved": True, "notes": "ok"}),
                        "metadata": "operator=web",
                    },
                )
                self.assertEqual(complete_response.status_code, 200)
                self.assertIn(
                    "Completed doc_operator.pdf / review",
                    complete_response.text,
                )

                audit_response = await client.get(
                    "/runs/run_web_operator/process-runtime/audit"
                )
                self.assertEqual(audit_response.status_code, 200)
                self.assertIn("Operator audit", audit_response.text)
                self.assertIn("process.output.put", audit_response.text)
                self.assertIn("web-panel", audit_response.text)
                self.assertIn("manual_complete_form", audit_response.text)

                pause_response = await client.post(
                    "/runs/run_web_operator/actions/pause?reason=web%20panel",
                    follow_redirects=False,
                )
                self.assertEqual(pause_response.status_code, 303)

                await service.complete_process_output(
                    run_id="run_web_operator",
                    document_id="doc_operator.pdf",
                    pipeline_id="web_operator",
                    process_id="export",
                    output=ProcessOutput(
                        artifacts=[
                            ArtifactRef(
                                id="export_pdf",
                                kind="export_file",
                                uri="s3://bucket/export.pdf",
                            )
                        ],
                        output_documents=[
                            OutputDocumentRef(
                                id="exported_doc",
                                title="Exported PDF",
                                document_type="exported_document",
                                media_type="application/pdf",
                                uri="s3://bucket/export.pdf",
                                artifact_id="export_pdf",
                                relation="exported",
                                metadata={"filename": "export.pdf"},
                            )
                        ],
                    ),
                )

                results_response = await client.get(
                    "/runs/run_web_operator/process-runtime/results"
                )
                self.assertEqual(results_response.status_code, 200)
                self.assertIn("Run results", results_response.text)
                self.assertIn("doc_operator.pdf", results_response.text)
                self.assertIn("approved", results_response.text)
                self.assertIn("output docs", results_response.text)

                output_documents_response = await client.get(
                    "/runs/run_web_operator/process-runtime/output-documents"
                )
                self.assertEqual(output_documents_response.status_code, 200)
                self.assertIn("Output documents", output_documents_response.text)
                self.assertIn("Exported PDF", output_documents_response.text)
                self.assertIn("exported_document", output_documents_response.text)
                self.assertIn("download", output_documents_response.text)

                reductions_response = await client.get(
                    "/runs/run_web_operator/process-runtime/reductions"
                )
                self.assertEqual(reductions_response.status_code, 200)
                self.assertIn("Run reductions", reductions_response.text)
                self.assertIn("review_decisions", reductions_response.text)
                self.assertIn("approved", reductions_response.text)

                lineage_response = await client.get(
                    "/runs/run_web_operator/process-runtime/lineage"
                )
                self.assertEqual(lineage_response.status_code, 200)
                self.assertIn("Document lineage", lineage_response.text)
                self.assertIn("doc_operator.pdf", lineage_response.text)
                self.assertIn("root", lineage_response.text)

        asyncio.run(fetch())
