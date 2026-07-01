from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

from fala.adapters import StepRunRequest, create_step_adapter
from fala.models import CarrierAdapterSpec


def python_step(request: StepRunRequest) -> dict:
    return {
        "carrier_id": request.carrier_id,
        "value": request.input["value"] + 1,
    }


class FalaStepAdapterTests(unittest.TestCase):
    def test_python_function_adapter_runs_ref(self) -> None:
        async def scenario() -> None:
            adapter = create_step_adapter("python_function")
            result = await adapter.run(
                StepRunRequest(
                    run_id="run_step",
                    process_id="process_step",
                    carrier_id="carrier_step",
                    adapter=CarrierAdapterSpec(
                        kind="python_function",
                        ref="tests.test_fala_step_adapters.python_step",
                    ),
                    input={"value": 2},
                )
            )
            self.assertEqual(
                result.output,
                {"carrier_id": "carrier_step", "value": 3},
            )

        asyncio.run(scenario())

    def test_subprocess_adapter_uses_input_and_output_manifests(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                script = root / "step.py"
                script.write_text(
                    """
import json
import os
from pathlib import Path

manifest = json.loads(Path(os.environ["FALA_STEP_MANIFEST"]).read_text())
output = Path(os.environ["FALA_STEP_OUTPUT_DIR"])
output.mkdir(parents=True, exist_ok=True)
(output / "result.json").write_text(json.dumps({
    "carrier_id": manifest["carrier_id"],
    "value": manifest["input"]["value"] + 1,
}))
print("done")
""".strip(),
                    encoding="utf-8",
                )

                adapter = create_step_adapter("subprocess")
                result = await adapter.run(
                    StepRunRequest(
                        run_id="run_step",
                        process_id="process_step",
                        carrier_id="carrier_step",
                        adapter=CarrierAdapterSpec(
                            kind="subprocess",
                            command=[sys.executable, str(script)],
                            timeout_seconds=5,
                        ),
                        input={"value": 2},
                        work_dir=root / "work",
                    )
                )

                manifest_path = root / "work" / "input" / "manifest.json"
                self.assertTrue(manifest_path.exists())
                self.assertEqual(result.output["value"], 3)
                self.assertEqual(result.stdout.strip(), "done")

        asyncio.run(scenario())

    def test_subprocess_adapter_resolves_and_redacts_env_secret_refs(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                script = root / "step.py"
                script.write_text(
                    """
import json
import os
from pathlib import Path

assert os.environ["TOKEN"] == "secret-value"
output = Path(os.environ["FALA_STEP_OUTPUT_DIR"])
output.mkdir(parents=True, exist_ok=True)
(output / "result.json").write_text(json.dumps({"ok": True}))
print(os.environ["TOKEN"])
""".strip(),
                    encoding="utf-8",
                )

                os.environ["FALA_TEST_TOKEN"] = "secret-value"
                try:
                    adapter = create_step_adapter("subprocess")
                    result = await adapter.run(
                        StepRunRequest(
                            run_id="run_secret",
                            process_id="process_secret",
                            adapter=CarrierAdapterSpec(
                                kind="subprocess",
                                command=[sys.executable, str(script)],
                                env={"TOKEN": "${env:FALA_TEST_TOKEN}"},
                                timeout_seconds=5,
                            ),
                            work_dir=root / "work",
                        )
                    )
                finally:
                    os.environ.pop("FALA_TEST_TOKEN", None)

                manifest = json.loads(
                    (root / "work" / "input" / "manifest.json").read_text(
                        encoding="utf-8"
                    )
                )
                self.assertEqual(
                    manifest["adapter"]["env"],
                    {"TOKEN": "${env:FALA_TEST_TOKEN}"},
                )
                self.assertEqual(result.stdout.strip(), "<redacted>")
                self.assertNotIn("secret-value", json.dumps(manifest))

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
