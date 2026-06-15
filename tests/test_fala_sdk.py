from __future__ import annotations

import io
import json
import os
import sys
import hashlib
import threading
import tempfile
import tomllib
import unittest
from pathlib import Path
from unittest.mock import patch

from fala.embedded import (
    EmbeddedRuntimeConfigError,
    RuntimeServiceConcurrencyError,
    SyncRuntimeDriver,
    resolve_embedded_runtime_config,
)
from fala.sdk import (
    ArtifactNotFoundError,
    JsonArtifact,
    JsonNeed,
    PROCESS_RUNTIME_EVENT_PREFIX,
    StepContract,
    artifact,
    artifact_root,
    build_step_env,
    emit_event,
    initial,
    latest_input_artifact,
    output,
    output_document,
    path_from_artifact,
    path_from_uri,
    read_json_artifact,
    read_needed_json,
    replay_step_manifest,
    require_artifact_path,
    run_step,
    run_stdio,
    stream_chunk,
    write_json_artifact,
    write_json,
)


class ProcessRuntimeSDKTests(unittest.TestCase):
    def test_run_stdio_reads_context_and_writes_process_output_json(self) -> None:
        stdin = io.StringIO(
            json.dumps(
                {
                    "input": {
                        "values": {
                            "initial": {
                                "source": "doc.pdf",
                            },
                        },
                    },
                }
            )
        )
        stdout = io.StringIO()

        def handler(context: dict) -> dict:
            return output(values={"source": initial(context)["source"]})

        with patch.object(sys, "stdin", stdin), patch.object(sys, "stdout", stdout):
            rc = run_stdio(handler)

        self.assertEqual(rc, 0)
        self.assertEqual(json.loads(stdout.getvalue())["values"], {"source": "doc.pdf"})

    def test_emit_event_writes_prefixed_json_line(self) -> None:
        stream = io.StringIO()

        emit_event(
            "process.progress",
            status="running",
            data={"percent": 50},
            stream=stream,
        )

        line = stream.getvalue().strip()
        self.assertTrue(line.startswith(PROCESS_RUNTIME_EVENT_PREFIX))
        payload = json.loads(line.removeprefix(PROCESS_RUNTIME_EVENT_PREFIX))
        self.assertEqual(payload["type"], "process.progress")
        self.assertEqual(payload["status"], "running")
        self.assertEqual(payload["data"], {"percent": 50})

    def test_stream_chunk_builds_chunk_payload(self) -> None:
        payload = stream_chunk(
            sequence=3,
            kind="page",
            values={"text": "hello"},
            metadata={"lang": "pl"},
        )

        self.assertEqual(payload["sequence"], 3)
        self.assertEqual(payload["kind"], "page")
        self.assertEqual(payload["values"], {"text": "hello"})
        self.assertEqual(payload["metadata"], {"lang": "pl"})
        self.assertEqual(payload["artifacts"], [])

    def test_output_can_include_typed_output_documents(self) -> None:
        payload = output(
            values={"status": "ok"},
            output_documents=[
                output_document(
                    id="redacted_doc",
                    document_type="redacted_document",
                    media_type="application/pdf",
                    uri="file:///tmp/redacted.pdf",
                    artifact_id="redacted_pdf",
                    relation="redacted",
                    values={"source": "source.pdf"},
                    metadata={"filename": "redacted.pdf"},
                )
            ],
        )

        self.assertEqual(payload["values"], {"status": "ok"})
        self.assertEqual(payload["output_documents"][0]["id"], "redacted_doc")
        self.assertEqual(
            payload["output_documents"][0]["document_type"],
            "redacted_document",
        )
        self.assertEqual(payload["output_documents"][0]["relation"], "redacted")

    def test_artifact_helpers_use_runtime_artifact_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            old_artifact_dir = os.environ.get("PROCESS_RUNTIME_ARTIFACT_DIR")
            os.environ["PROCESS_RUNTIME_ARTIFACT_DIR"] = tmp
            try:
                root = artifact_root({"run_id": "run", "document_id": "doc.pdf"}, "extract")
                path = write_json(root / "payload.json", {"ok": True})
                ref = artifact("payload", path)
                expected_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
                expected_size = path.stat().st_size
                value = read_needed_json(
                    {"extract": {"artifacts": [ref]}},
                    "extract",
                    "payload",
                )
            finally:
                if old_artifact_dir is None:
                    os.environ.pop("PROCESS_RUNTIME_ARTIFACT_DIR", None)
                else:
                    os.environ["PROCESS_RUNTIME_ARTIFACT_DIR"] = old_artifact_dir

        self.assertEqual(root, Path(tmp))
        self.assertEqual(value, {"ok": True})
        self.assertEqual(ref["metadata"]["sha256"], expected_sha256)
        self.assertEqual(ref["metadata"]["size_bytes"], expected_size)
        self.assertEqual(ref["metadata"]["filename"], "payload.json")

    def test_input_artifact_helpers_read_latest_json_and_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            older = root / "older.json"
            newer = root / "newer.json"
            bad = root / "bad.json"
            older.write_text(json.dumps({"version": 1}), encoding="utf-8")
            newer.write_text(json.dumps({"version": 2}), encoding="utf-8")
            bad.write_text("{not json", encoding="utf-8")
            context = {
                "input": {
                    "artifacts": [
                        artifact("payload", older),
                        artifact("payload", newer),
                        artifact("bad", bad),
                    ]
                }
            }

            latest = latest_input_artifact(context, "payload")
            path = require_artifact_path(context, "payload")
            value = read_json_artifact(context, "payload")
            written = write_json_artifact(
                context,
                "emit",
                "result",
                {"ok": True},
                filename="result.json",
            )

            self.assertEqual(latest["uri"], newer.resolve().as_uri())
            self.assertEqual(path, newer.resolve())
            self.assertEqual(value, {"version": 2})
            self.assertEqual(written["kind"], "result")
            self.assertIn("sha256", written["metadata"])
            with self.assertRaises(ArtifactNotFoundError):
                latest_input_artifact(context, "missing")
            with self.assertRaises(json.JSONDecodeError):
                read_json_artifact(context, "bad")

    def test_read_json_artifact_resolves_content_addressed_uri(self) -> None:
        from fala.artifacts import FileArtifactStore

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "payload.json"
            source.write_text(json.dumps({"stored": True}), encoding="utf-8")
            store = FileArtifactStore(root / "artifact-store")
            ref = store.put_file(kind="payload", path=source)
            old_root = os.environ.get("FALA_ARTIFACT_STORE_ROOT")
            os.environ["FALA_ARTIFACT_STORE_ROOT"] = str(store.root)
            try:
                value = read_json_artifact(
                    {"input": {"artifacts": [ref.model_dump(mode="json")]}},
                    "payload",
                )
            finally:
                if old_root is None:
                    os.environ.pop("FALA_ARTIFACT_STORE_ROOT", None)
                else:
                    os.environ["FALA_ARTIFACT_STORE_ROOT"] = old_root

        self.assertEqual(value, {"stored": True})

    def test_build_step_env_sets_artifact_paths_without_domain_names(self) -> None:
        env = build_step_env(
            process_artifact_root="/tmp/process-artifacts",
            artifact_store_root="/tmp/artifact-store",
            artifact_store="file:/tmp/artifact-store",
            artifact_cache_root="/tmp/cache",
            base_env={"KEEP": "1"},
        )

        self.assertEqual(env["KEEP"], "1")
        self.assertEqual(env["PROCESS_RUNTIME_ARTIFACT_ROOT"], "/tmp/process-artifacts")
        self.assertEqual(env["FALA_ARTIFACT_STORE_ROOT"], "/tmp/artifact-store")
        self.assertEqual(env["FALA_ARTIFACT_STORE"], "file:/tmp/artifact-store")
        self.assertEqual(env["FALA_ARTIFACT_CACHE_ROOT"], "/tmp/cache")

    def test_run_step_writes_manifest_and_replays_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload_path = root / "payload.json"
            payload_path.write_text(json.dumps([{"id": 1}]), encoding="utf-8")
            artifact_dir = root / "artifacts"
            context = {
                "pipeline_id": "demo_pipeline",
                "run_id": "run_1",
                "document_id": "doc.pdf",
                "process_id": "enrich",
                "attempt": 2,
                "input": {
                    "values": {
                        "initial": {},
                        "needs": {
                            "ingest": {
                                "artifacts": [artifact("payload", payload_path)]
                            }
                        },
                    },
                    "artifacts": [],
                },
            }
            contract = StepContract(
                process_id="enrich",
                needs={"payload": JsonNeed("ingest", "payload")},
                outputs={"result": JsonArtifact("result", "result.json")},
            )

            def handler(step_context):
                rows = step_context.needs.json("payload", default=[])
                rows.append({"id": 2})
                return step_context.complete(
                    values={"status": "ok", "rows": len(rows)},
                    artifacts={"result": rows},
                )

            old_artifact_dir = os.environ.get("PROCESS_RUNTIME_ARTIFACT_DIR")
            os.environ["PROCESS_RUNTIME_ARTIFACT_DIR"] = str(artifact_dir)
            stdin = io.StringIO(json.dumps(context))
            stdout = io.StringIO()
            try:
                with patch.object(sys, "stdin", stdin), patch.object(sys, "stdout", stdout):
                    rc = run_step(contract, handler)
            finally:
                if old_artifact_dir is None:
                    os.environ.pop("PROCESS_RUNTIME_ARTIFACT_DIR", None)
                else:
                    os.environ["PROCESS_RUNTIME_ARTIFACT_DIR"] = old_artifact_dir

            output_payload = json.loads(stdout.getvalue())
            manifest_path = artifact_dir / "step_run_manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            replay = replay_step_manifest(
                manifest_path,
                [
                    sys.executable,
                    "-c",
                    (
                        "import json,sys; "
                        "ctx=json.loads(sys.stdin.read()); "
                        "print(json.dumps({'values': {'process_id': ctx['process_id']}, "
                        "'artifacts': [], 'metadata': {}}))"
                    ),
                ],
            )

        self.assertEqual(rc, 0)
        self.assertEqual(output_payload["values"], {"status": "ok", "rows": 2})
        self.assertEqual(output_payload["artifacts"][0]["kind"], "result")
        self.assertIn("step_run_manifest", output_payload["metadata"])
        self.assertEqual(manifest["schema"], "fala.step_run_manifest.v1")
        self.assertEqual(manifest["run_id"], "run_1")
        self.assertEqual(manifest["needs"]["payload"]["artifact"]["kind"], "payload")
        self.assertEqual(manifest["outputs"][0]["kind"], "result")
        self.assertTrue(replay["ok"], replay)
        self.assertEqual(replay["output"]["values"], {"process_id": "enrich"})

    def test_path_from_uri_resolves_fala_artifact_store_refs(self) -> None:
        from fala.artifacts import FileArtifactStore

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.txt"
            source.write_text("stored", encoding="utf-8")
            store = FileArtifactStore(root / "artifact-store")
            ref = store.put_file(kind="text", path=source)
            old_root = os.environ.get("FALA_ARTIFACT_STORE_ROOT")
            os.environ["FALA_ARTIFACT_STORE_ROOT"] = str(store.root)
            try:
                resolved = path_from_uri(ref.uri)
                resolved_from_artifact = path_from_artifact(ref)
                self.assertIsNotNone(resolved)
                self.assertEqual(resolved, resolved_from_artifact)
                assert resolved is not None
                content = resolved.read_text(encoding="utf-8")
            finally:
                if old_root is None:
                    os.environ.pop("FALA_ARTIFACT_STORE_ROOT", None)
                else:
                    os.environ["FALA_ARTIFACT_STORE_ROOT"] = old_root

        self.assertEqual(content, "stored")

    def test_path_from_uri_prefers_fala_artifact_store_file_target(self) -> None:
        from fala.artifacts import FileArtifactStore

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.txt"
            source.write_text("from file target", encoding="utf-8")
            store = FileArtifactStore(root / "artifact-store")
            ref = store.put_file(kind="text", path=source)
            with patch.dict(
                os.environ,
                {
                    "FALA_ARTIFACT_STORE": f"file:{store.root}",
                    "FALA_ARTIFACT_STORE_ROOT": str(root / "ignored"),
                },
            ):
                resolved = path_from_uri(ref.uri)

            self.assertIsNotNone(resolved)
            assert resolved is not None
            self.assertEqual(resolved.read_text(encoding="utf-8"), "from file target")

    def test_path_from_uri_downloads_from_s3_artifact_store(self) -> None:
        payload = b"from s3 target"
        digest = hashlib.sha256(payload).hexdigest()
        uri = f"fala-artifact://sha256/{digest}"

        class FakeS3Client:
            def __init__(self) -> None:
                self.calls: list[tuple[str, str]] = []

            def download_fileobj(self, bucket, key, handle) -> None:
                self.calls.append((bucket, key))
                handle.write(payload)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake = FakeS3Client()
            with (
                patch.dict(
                    os.environ,
                    {
                        "FALA_ARTIFACT_STORE": "s3://fala-bucket/prefix",
                        "FALA_ARTIFACT_CACHE_ROOT": str(root / "cache"),
                    },
                ),
                patch("fala.sdk._s3_client", return_value=fake),
            ):
                resolved = path_from_uri(uri)
                cached = path_from_uri(uri)

            self.assertIsNotNone(resolved)
            assert resolved is not None
            self.assertEqual(cached, resolved)
            self.assertEqual(resolved.read_bytes(), payload)
        self.assertEqual(
            fake.calls,
            [("fala-bucket", f"prefix/blobs/sha256/{digest[:2]}/{digest}")],
        )

    def test_embedded_runtime_config_resolves_absolute_defaults_and_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            override = root / "override"
            env = {
                "APP_FALA_DB_PATH": str(override / "runtime.sqlite"),
                "APP_FALA_ARTIFACT_STORE_ROOT": str(override / "store"),
                "APP_FALA_PROCESS_ARTIFACT_ROOT": str(override / "process"),
            }

            defaults = resolve_embedded_runtime_config(
                prefix="APP_FALA",
                default_root=root / "default",
                env={},
            )
            overridden = resolve_embedded_runtime_config(
                prefix="APP_FALA",
                default_root=root / "default",
                env=env,
            )
            named_default = resolve_embedded_runtime_config(
                prefix="APP_FALA",
                default_root=root / "default",
                default_db_filename="control_plane.db",
                env={},
            )

        self.assertTrue(defaults.db_path.is_absolute())
        self.assertEqual(defaults.db_path.name, "fala.sqlite")
        self.assertEqual(named_default.db_path.name, "control_plane.db")
        self.assertEqual(overridden.db_path, (override / "runtime.sqlite").resolve())
        self.assertEqual(overridden.artifact_store_root, (override / "store").resolve())
        self.assertEqual(overridden.process_artifact_root, (override / "process").resolve())

    def test_embedded_runtime_config_supports_explicit_env_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            public = root / "public"
            env = {
                "PUBLIC_FALA_WORK": str(public / "work"),
                "PUBLIC_FALA_DB": str(public / "runtime.sqlite"),
                "PUBLIC_FALA_STORE": str(public / "store"),
                "PUBLIC_FALA_PROCESS": str(public / "process"),
            }

            config = resolve_embedded_runtime_config(
                prefix="APP_FALA",
                default_root=root / "default",
                env=env,
                aliases={
                    "work_root": "PUBLIC_FALA_WORK",
                    "db_path": ["PUBLIC_FALA_DB"],
                    "artifact_store_root": "PUBLIC_FALA_STORE",
                    "process_artifact_root": "PUBLIC_FALA_PROCESS",
                },
            )

        self.assertEqual(config.work_root, (public / "work").resolve())
        self.assertEqual(config.db_path, (public / "runtime.sqlite").resolve())
        self.assertEqual(config.artifact_store_root, (public / "store").resolve())
        self.assertEqual(config.process_artifact_root, (public / "process").resolve())

    def test_embedded_runtime_config_rejects_blank_and_relative_env_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(EmbeddedRuntimeConfigError):
                resolve_embedded_runtime_config(
                    prefix="APP_FALA",
                    default_root=root,
                    env={"APP_FALA_DB_PATH": "  "},
                )
            with self.assertRaises(EmbeddedRuntimeConfigError):
                resolve_embedded_runtime_config(
                    prefix="APP_FALA",
                    default_root=root,
                    env={"APP_FALA_DB_PATH": "relative.sqlite"},
                )
            with self.assertRaises(EmbeddedRuntimeConfigError):
                resolve_embedded_runtime_config(
                    prefix="APP_FALA",
                    default_root=root,
                    env={"PUBLIC_FALA_DB": "  "},
                    aliases={"db_path": "PUBLIC_FALA_DB"},
                )
            with self.assertRaises(EmbeddedRuntimeConfigError):
                resolve_embedded_runtime_config(
                    prefix="APP_FALA",
                    default_root=root,
                    env={"PUBLIC_FALA_DB": "relative.sqlite"},
                    aliases={"db_path": "PUBLIC_FALA_DB"},
                )
            with self.assertRaisesRegex(EmbeddedRuntimeConfigError, "conflicts"):
                resolve_embedded_runtime_config(
                    prefix="APP_FALA",
                    default_root=root,
                    env={
                        "APP_FALA_DB_PATH": str(root / "runtime.sqlite"),
                        "PUBLIC_FALA_DB": str(root / "other.sqlite"),
                    },
                    aliases={"db_path": "PUBLIC_FALA_DB"},
                )

    def test_sync_runtime_driver_supports_sequential_reuse_and_blocks_concurrent_reuse(self) -> None:
        driver = SyncRuntimeDriver({"value": 2})
        self.assertEqual(driver.run(lambda runtime: runtime["value"] + 1), 3)

        started = threading.Event()
        release = threading.Event()
        results: list[str] = []

        def blocking_operation(_runtime):
            async def run():
                started.set()
                await __import__("asyncio").to_thread(release.wait)
                return "done"

            return run()

        thread = threading.Thread(
            target=lambda: results.append(driver.run(blocking_operation)),
        )
        thread.start()
        self.assertTrue(started.wait(timeout=2.0))
        with self.assertRaises(RuntimeServiceConcurrencyError):
            driver.run(lambda _runtime: "second")
        release.set()
        thread.join(timeout=2.0)

        self.assertEqual(results, ["done"])

    def test_package_declares_license_metadata(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        pyproject = tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))

        self.assertTrue((repo_root / "LICENSE").exists())
        self.assertEqual(pyproject["project"]["license"], "MIT")
        self.assertIn("LICENSE", pyproject["project"]["license-files"])


if __name__ == "__main__":
    unittest.main()
