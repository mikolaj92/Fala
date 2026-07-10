from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fala import Impulse, Run, RuntimeCommand, RuntimeEvent, Correlator


class ImpulseRuntimeTests(unittest.TestCase):
    def test_non_document_impulse_correlation_path_uses_runtime_backend(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                backend = Correlator(Path(tmp_dir) / "fala.sqlite")
                await backend.put_run(Run(id="run_impulse"))
                impulse = Impulse(
                    run_id="run_impulse",
                    impulse_type="arbitration_case",
                    payload={"claim_id": "CLM-1"},
                    metadata={"tenant": "acme"},
                )
                command = RuntimeCommand(
                    run_id="run_impulse",
                    command_type="impulse.accept",
                    idempotency_key="run_impulse:impulse.accept:CLM-1",
                    actor="operator:mika",
                    payload={"impulse_id": impulse.id},
                )
                event = RuntimeEvent(
                    run_id="run_impulse",
                    impulse_id=impulse.id,
                    event_type="impulse.accepted",
                    payload={"accepted": True},
                )

                await backend.put_impulse(impulse)
                submission = await backend.submit_command(command, events=[event])
                stored = await backend.get_impulse(
                    run_id="run_impulse",
                    impulse_id=impulse.id,
                )
                events = await backend.list_events(run_id="run_impulse")

                self.assertEqual(stored, impulse)
                self.assertFalse(submission.replayed)
                self.assertEqual(events[0].impulse_id, impulse.id)
                serialized = json.dumps(
                    {
                        "impulse": stored.model_dump(mode="json"),
                        "events": [item.model_dump(mode="json") for item in events],
                    }
                )
                self.assertNotIn("document_id", serialized)
                self.assertNotIn("document_type", serialized)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
