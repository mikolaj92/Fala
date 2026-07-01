from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from fala.runtime_backend import (
    Carrier,
    CarrierProcessStatus,
    Process,
    RuntimeCommand,
    RuntimeEvent,
    Run,
    SQLiteRuntimeBackend,
)

from tests.runtime_backend_conformance import assert_runtime_backend_conformance


class SQLiteRuntimeBackendConformanceTests(unittest.TestCase):
    def test_sqlite_runtime_backend_satisfies_conformance_suite(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                backend = SQLiteRuntimeBackend(Path(tmp_dir) / "runtime.sqlite")
                await assert_runtime_backend_conformance(backend)

        asyncio.run(scenario())

    def test_sqlite_runtime_backend_recovers_state_after_restart(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                path = Path(tmp_dir) / "runtime.sqlite"
                first = SQLiteRuntimeBackend(path)
                run = Run(id="run_restart")
                carrier = Carrier(
                    id="carrier_restart",
                    run_id=run.id,
                    carrier_type="case",
                )
                process = Process(
                    id="process_restart",
                    run_id=run.id,
                    carrier_id=carrier.id,
                    process_type="score",
                    status=CarrierProcessStatus.ready,
                )
                command = RuntimeCommand(
                    run_id=run.id,
                    command_type="carrier.accept",
                    idempotency_key="run_restart:carrier.accept:carrier_restart",
                )
                await first.put_run(run)
                await first.put_carrier(carrier)
                await first.put_process(process)
                first_submission = await first.submit_command(
                    command,
                    events=[
                        RuntimeEvent(
                            run_id=run.id,
                            carrier_id=carrier.id,
                            event_type="carrier.accepted",
                        )
                    ],
                )

                second = SQLiteRuntimeBackend(path)
                self.assertEqual(await second.get_run(run_id=run.id), run)
                self.assertEqual(
                    await second.get_carrier(
                        run_id=run.id,
                        carrier_id=carrier.id,
                    ),
                    carrier,
                )
                events = await second.list_events(run_id=run.id)
                self.assertEqual([event.sequence for event in events], [1])
                self.assertEqual(events[0].command_id, first_submission.command.id)
                replay = await second.submit_command(
                    command.model_copy(update={"id": "command_restart_replay"}),
                    events=[
                        RuntimeEvent(
                            run_id=run.id,
                            carrier_id=carrier.id,
                            event_type="carrier.accepted",
                        )
                    ],
                )
                self.assertTrue(replay.replayed)
                self.assertEqual(replay.events, [])
                claimed = await second.claim_next_ready_process(
                    run_id=run.id,
                    worker_id="worker_restart",
                )
                self.assertIsNotNone(claimed)
                assert claimed is not None
                self.assertEqual(claimed.status, CarrierProcessStatus.running)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
