from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from fala.sdk import (
    INJECTED_INPUT_KEYS,
    declared_inputs,
    find_reaction,
    find_output_reaction,
    input_values,
    load_manifest,
    conduction,
    output,
    output_reactions,
    output_metadata,
    run_manifest_effector,
    upstream_reactions,
)


class FalaSdkTests(unittest.TestCase):
    def test_manifest_helpers_read_input_and_conduction(self) -> None:
        manifest = {
            "input": {
                "source": "hello",
                "conduction": {"ingest": {"chars": 5}},
            }
        }

        self.assertEqual(input_values(manifest)["source"], "hello")
        self.assertEqual(conduction(manifest)["ingest"]["chars"], 5)
        self.assertEqual(
            output(values={"ok": True}),
            {
                "values": {"ok": True},
                "associations": [],
                "reactions": [],
                "metadata": {},
            },
        )

    def test_declared_inputs_strips_injected_keys(self) -> None:
        manifest = {
            "input": {
                "source": "hello",
                "conduction": {"a": {}},
                "upstream_reactions": [{"kind": "x"}],
            }
        }
        # Only the authored value survives; Fala's injected keys are stripped.
        self.assertEqual(declared_inputs(manifest), {"source": "hello"})
        # The manifest itself is left untouched.
        self.assertEqual(
            manifest["input"],
            {
                "source": "hello",
                "conduction": {"a": {}},
                "upstream_reactions": [{"kind": "x"}],
            },
        )

    def test_declared_inputs_defaults_empty(self) -> None:
        # Missing / malformed input yields {}, matching input_values.
        self.assertEqual(declared_inputs({}), {})
        self.assertEqual(declared_inputs({"input": None}), {})
        self.assertEqual(declared_inputs({"input": ["not-a-mapping"]}), {})

    def test_injected_input_keys_constant(self) -> None:
        self.assertEqual(
            INJECTED_INPUT_KEYS, frozenset({"conduction", "upstream_reactions"})
        )
        self.assertIsInstance(INJECTED_INPUT_KEYS, frozenset)

    def test_upstream_reactions_reads_list_or_defaults_empty(self) -> None:
        manifest = {
            "input": {
                "upstream_reactions": [
                    {"kind": "a"},
                    "not-a-mapping",
                    {"kind": "b"},
                ]
            }
        }
        self.assertEqual(
            upstream_reactions(manifest),
            [{"kind": "a"}, {"kind": "b"}],
        )
        # Absent / opted-out correlation_paths and root effectors see an empty list, never a KeyError.
        self.assertEqual(upstream_reactions({"input": {}}), [])
        self.assertEqual(upstream_reactions({}), [])

    def test_find_reaction_returns_latest_match_or_none(self) -> None:
        manifest = {
            "input": {
                "upstream_reactions": [
                    {"kind": "draft", "path": "a"},
                    {"kind": "draft", "path": "b"},
                    {"kind": "final", "path": "c"},
                ]
            }
        }
        # Newest-first scan: the later "draft" (b) shadows the earlier one (a).
        self.assertEqual(find_reaction(manifest, "draft"), {"kind": "draft", "path": "b"})
        self.assertEqual(find_reaction(manifest, "final"), {"kind": "final", "path": "c"})
        # Missing kind and an absent / opted-out reaction list both yield None.
        self.assertIsNone(find_reaction(manifest, "missing"))
        self.assertIsNone(find_reaction({}, "draft"))

    def test_output_reactions_reads_envelope_list_or_defaults_empty(self) -> None:
        # The host-side twin of upstream_reactions: it reads an effector's own output
        # envelope (as produced by output()), not a downstream effector's input.
        effector_output = {
            "reactions": [
                {"kind": "report"},
                "not-a-mapping",
                {"kind": "manifest"},
            ]
        }
        self.assertEqual(
            output_reactions(effector_output),
            [{"kind": "report"}, {"kind": "manifest"}],
        )
        # An effector that emitted nothing, or an absent / malformed envelope, is empty.
        self.assertEqual(output_reactions(output()), [])
        self.assertEqual(output_reactions({}), [])
        self.assertEqual(output_reactions({"reactions": "nope"}), [])

    def test_find_output_reaction_returns_latest_match_or_none(self) -> None:
        effector_output = output(
            reactions=[
                {"kind": "draft", "path": "a"},
                {"kind": "draft", "path": "b"},
                {"kind": "final", "path": "c"},
            ]
        )
        # Same newest-first rule as find_reaction: the later draft (b) wins.
        self.assertEqual(
            find_output_reaction(effector_output, "draft"),
            {"kind": "draft", "path": "b"},
        )
        self.assertEqual(
            find_output_reaction(effector_output, "final"),
            {"kind": "final", "path": "c"},
        )
        self.assertIsNone(find_output_reaction(effector_output, "missing"))
        self.assertIsNone(find_output_reaction({}, "draft"))

    def test_output_metadata_reads_envelope_metadata_or_defaults_empty(self) -> None:
        self.assertEqual(
            output_metadata(output(metadata={"telemetry": {"ms": 12}})),
            {"telemetry": {"ms": 12}},
        )
        # Absent / malformed metadata round-trips to {}, never a KeyError.
        self.assertEqual(output_metadata({}), {})
        self.assertEqual(output_metadata({"metadata": "nope"}), {})

    def test_run_manifest_effector_writes_result_json(self) -> None:
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
                "FALA_EFFECTOR_MANIFEST": str(manifest_path),
                "FALA_EFFECTOR_OUTPUT_DIR": str(output_dir),
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
                self.assertEqual(run_manifest_effector(handler), 0)
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
