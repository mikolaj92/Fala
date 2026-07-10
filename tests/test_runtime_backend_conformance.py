from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from fala.runtime_backend import (
    Impulse,
    ProcessStatus,
    Process,
    RuntimeCommand,
    RuntimeEvent,
    Run,
    Correlator,
)

from tests.runtime_backend_conformance import assert_runtime_backend_conformance


class CorrelatorConformanceTests(unittest.TestCase):
    def test_sqlite_runtime_backend_satisfies_conformance_suite(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                backend = Correlator(Path(tmp_dir) / "runtime.sqlite")
                await assert_runtime_backend_conformance(backend)

        asyncio.run(scenario())

    def test_sqlite_runtime_backend_recovers_state_after_restart(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                path = Path(tmp_dir) / "runtime.sqlite"
                first = Correlator(path)
                run = Run(id="run_restart")
                impulse = Impulse(
                    id="impulse_restart",
                    run_id=run.id,
                    impulse_type="case",
                )
                process = Process(
                    id="process_restart",
                    run_id=run.id,
                    impulse_id=impulse.id,
                    process_type="score",
                    status=ProcessStatus.ready,
                )
                command = RuntimeCommand(
                    run_id=run.id,
                    command_type="impulse.accept",
                    idempotency_key="run_restart:impulse.accept:impulse_restart",
                )
                await first.put_run(run)
                await first.put_impulse(impulse)
                await first.put_process(process)
                first_submission = await first.submit_command(
                    command,
                    events=[
                        RuntimeEvent(
                            run_id=run.id,
                            impulse_id=impulse.id,
                            event_type="impulse.accepted",
                        )
                    ],
                )

                second = Correlator(path)
                self.assertEqual(await second.get_run(run_id=run.id), run)
                self.assertEqual(
                    await second.get_impulse(
                        run_id=run.id,
                        impulse_id=impulse.id,
                    ),
                    impulse,
                )
                events = await second.list_events(run_id=run.id)
                self.assertEqual([event.sequence for event in events], [1])
                self.assertEqual(events[0].command_id, first_submission.command.id)
                replay = await second.submit_command(
                    command.model_copy(update={"id": "command_restart_replay"}),
                    events=[
                        RuntimeEvent(
                            run_id=run.id,
                            impulse_id=impulse.id,
                            event_type="impulse.accepted",
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
                self.assertEqual(claimed.status, ProcessStatus.running)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
