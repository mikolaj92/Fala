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

from fala import Carrier, Run, RuntimeCommand, RuntimeEvent, SQLiteRuntimeBackend


class CarrierRuntimeTests(unittest.TestCase):
    def test_non_document_carrier_flow_uses_runtime_backend(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                backend = SQLiteRuntimeBackend(Path(tmp_dir) / "fala.sqlite")
                await backend.put_run(Run(id="run_carrier"))
                carrier = Carrier(
                    run_id="run_carrier",
                    carrier_type="arbitration_case",
                    payload={"claim_id": "CLM-1"},
                    metadata={"tenant": "acme"},
                )
                command = RuntimeCommand(
                    run_id="run_carrier",
                    command_type="carrier.accept",
                    idempotency_key="run_carrier:carrier.accept:CLM-1",
                    actor="operator:mika",
                    payload={"carrier_id": carrier.id},
                )
                event = RuntimeEvent(
                    run_id="run_carrier",
                    carrier_id=carrier.id,
                    event_type="carrier.accepted",
                    payload={"accepted": True},
                )

                await backend.put_carrier(carrier)
                submission = await backend.submit_command(command, events=[event])
                stored = await backend.get_carrier(
                    run_id="run_carrier",
                    carrier_id=carrier.id,
                )
                events = await backend.list_events(run_id="run_carrier")

                self.assertEqual(stored, carrier)
                self.assertFalse(submission.replayed)
                self.assertEqual(events[0].carrier_id, carrier.id)
                serialized = json.dumps(
                    {
                        "carrier": stored.model_dump(mode="json"),
                        "events": [item.model_dump(mode="json") for item in events],
                    }
                )
                self.assertNotIn("document_id", serialized)
                self.assertNotIn("document_type", serialized)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
