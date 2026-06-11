from __future__ import annotations

import io
import json
import os
import sys
import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fala.sdk import (
    PROCESS_RUNTIME_EVENT_PREFIX,
    artifact,
    artifact_root,
    emit_event,
    initial,
    output,
    output_document,
    path_from_artifact,
    path_from_uri,
    read_needed_json,
    run_stdio,
    stream_chunk,
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


if __name__ == "__main__":
    unittest.main()
