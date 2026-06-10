from __future__ import annotations

import io
import json
import os
import sys
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
    read_needed_json,
    run_stdio,
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

    def test_artifact_helpers_use_runtime_artifact_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            old_artifact_dir = os.environ.get("PROCESS_RUNTIME_ARTIFACT_DIR")
            os.environ["PROCESS_RUNTIME_ARTIFACT_DIR"] = tmp
            try:
                root = artifact_root({"run_id": "run", "document_id": "doc.pdf"}, "extract")
                path = write_json(root / "payload.json", {"ok": True})
                ref = artifact("payload", path)
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


if __name__ == "__main__":
    unittest.main()
