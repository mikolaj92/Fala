from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from fala.sdk import (
    find_artifact,
    input_values,
    load_manifest,
    needs,
    output,
    run_manifest_step,
    upstream_artifacts,
)


class FalaSdkTests(unittest.TestCase):
    def test_manifest_helpers_read_input_and_needs(self) -> None:
        manifest = {
            "input": {
                "source": "hello",
                "needs": {"ingest": {"chars": 5}},
            }
        }

        self.assertEqual(input_values(manifest)["source"], "hello")
        self.assertEqual(needs(manifest)["ingest"]["chars"], 5)
        self.assertEqual(
            output(values={"ok": True}),
            {
                "values": {"ok": True},
                "observations": [],
                "artifacts": [],
                "metadata": {},
            },
        )

    def test_upstream_artifacts_reads_list_or_defaults_empty(self) -> None:
        manifest = {
            "input": {
                "upstream_artifacts": [
                    {"kind": "a"},
                    "not-a-mapping",
                    {"kind": "b"},
                ]
            }
        }
        self.assertEqual(
            upstream_artifacts(manifest),
            [{"kind": "a"}, {"kind": "b"}],
        )
        # Absent / opted-out flows and root steps see an empty list, never a KeyError.
        self.assertEqual(upstream_artifacts({"input": {}}), [])
        self.assertEqual(upstream_artifacts({}), [])

    def test_find_artifact_returns_latest_match_or_none(self) -> None:
        manifest = {
            "input": {
                "upstream_artifacts": [
                    {"kind": "draft", "path": "a"},
                    {"kind": "draft", "path": "b"},
                    {"kind": "final", "path": "c"},
                ]
            }
        }
        # Newest-first scan: the later "draft" (b) shadows the earlier one (a).
        self.assertEqual(find_artifact(manifest, "draft"), {"kind": "draft", "path": "b"})
        self.assertEqual(find_artifact(manifest, "final"), {"kind": "final", "path": "c"})
        # Missing kind and an absent / opted-out artifact list both yield None.
        self.assertIsNone(find_artifact(manifest, "missing"))
        self.assertIsNone(find_artifact({}, "draft"))

    def test_run_manifest_step_writes_result_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            input_dir = root / "input"
            output_dir = root / "output"
            input_dir.mkdir()
            output_dir.mkdir()
            manifest_path = input_dir / "manifest.json"
            manifest_path.write_text(
                json.dumps({"input": {"value": 2}}),
                encoding="utf-8",
            )

            env = {
                "FALA_STEP_MANIFEST": str(manifest_path),
                "FALA_STEP_OUTPUT_DIR": str(output_dir),
            }

            self.assertEqual(load_manifest(env)["input"]["value"], 2)

            def handler(manifest: dict) -> dict:
                return output(values={"value": input_values(manifest)["value"] + 1})

            old_env = {}
            import os

            for key, value in env.items():
                old_env[key] = os.environ.get(key)
                os.environ[key] = value
            try:
                self.assertEqual(run_manifest_step(handler), 0)
            finally:
                for key, value in old_env.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

            result = json.loads((output_dir / "result.json").read_text())
            self.assertEqual(result["values"]["value"], 3)


if __name__ == "__main__":
    unittest.main()
