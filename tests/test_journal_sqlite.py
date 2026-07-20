"""SqliteJournal thin wrap tests (PR4 / #76)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from fala.journal import CommandUnit, JournalBatch, StateFact
from fala.journal.sqlite import SqliteJournal
from fala.journal.types import ClaimRequest
from fala.runtime_backend import (
    Process,
    ProcessStatus,
    Run,
    RuntimeCommand,
    RuntimeEvent,
)


class SqliteJournalTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "state.sqlite"
        self.journal = SqliteJournal(self.db)

    async def asyncTearDown(self) -> None:
        self._tmp.cleanup()

    async def test_runtime_uri_and_create_run(self) -> None:
        self.assertTrue(self.journal.runtime_uri.startswith("sqlite://"))
        run = Run(id="run_1")
        cmd = RuntimeCommand(
            run_id="run_1",
            command_type="run.create",
            idempotency_key="create",
        )
        result = await self.journal.append_batch(
            JournalBatch(
                run_id="run_1",
                units=[
                    CommandUnit(
                        command=cmd,
                        events=[
                            RuntimeEvent(run_id="run_1", event_type="run.created")
                        ],
                        facts=[
                            StateFact(
                                entity="run",
                                op="upsert",
                                key={"id": "run_1"},
                                body=run.model_dump(mode="json"),
                            )
                        ],
                    )
                ],
            )
        )
        self.assertFalse(result.replayed)
        self.assertEqual(result.units[0].events[0].sequence, 1)

        replay = await self.journal.append_batch(
            JournalBatch(
                run_id="run_1",
                units=[
                    CommandUnit(
                        command=cmd,
                        events=[
                            RuntimeEvent(run_id="run_1", event_type="run.created")
                        ],
                    )
                ],
            )
        )
        self.assertTrue(replay.replayed)
        self.assertEqual(replay.units[0].events, [])

    async def test_schedule_and_claim(self) -> None:
        await self.journal.append_batch(
            JournalBatch(
                run_id="run_1",
                units=[
                    CommandUnit(
                        command=RuntimeCommand(
                            run_id="run_1",
                            command_type="run.create",
                            idempotency_key="c",
                        ),
                        events=[
                            RuntimeEvent(run_id="run_1", event_type="run.created")
                        ],
                        facts=[
                            StateFact(
                                entity="run",
                                op="upsert",
                                key={"id": "run_1"},
                                body=Run(id="run_1").model_dump(mode="json"),
                            )
                        ],
                    )
                ],
            )
        )
        process = Process(
            id="p1",
            run_id="run_1",
            process_type="effector",
            status=ProcessStatus.ready,
        )
        await self.journal.append_batch(
            JournalBatch(
                run_id="run_1",
                units=[
                    CommandUnit(
                        command=RuntimeCommand(
                            run_id="run_1",
                            command_type="process.schedule",
                            idempotency_key="sched",
                            payload={"process_id": "p1"},
                        ),
                        events=[
                            RuntimeEvent(
                                run_id="run_1",
                                event_type="process.scheduled",
                                process_id="p1",
                            )
                        ],
                        facts=[
                            StateFact(
                                entity="process",
                                op="upsert",
                                key={"id": "p1"},
                                body=process.model_dump(mode="json"),
                            )
                        ],
                    )
                ],
            )
        )
        claimed = await self.journal.claim_next(
            ClaimRequest(worker_id="w1", run_id="run_1", lease_seconds=60)
        )
        self.assertIsNotNone(claimed.process)
        assert claimed.process is not None
        self.assertEqual(claimed.process.status, ProcessStatus.running)
        self.assertEqual(claimed.process.id, "p1")

        events = await self.journal.list_events(run_id="run_1")
        self.assertTrue(any(e.event_type == "process.claimed" for e in events))

    async def test_non_leading_units_ignored_for_write(self) -> None:
        """Leading unit only — extra ready unit does not become a separate command."""
        await self.journal.append_batch(
            JournalBatch(
                run_id="run_1",
                units=[
                    CommandUnit(
                        command=RuntimeCommand(
                            run_id="run_1",
                            command_type="run.create",
                            idempotency_key="c",
                        ),
                        events=[
                            RuntimeEvent(run_id="run_1", event_type="run.created")
                        ],
                        facts=[
                            StateFact(
                                entity="run",
                                op="upsert",
                                key={"id": "run_1"},
                                body=Run(id="run_1").model_dump(mode="json"),
                            )
                        ],
                    ),
                    CommandUnit(
                        command=RuntimeCommand(
                            run_id="run_1",
                            command_type="process.ready",
                            idempotency_key="ghost-ready",
                        ),
                        events=[
                            RuntimeEvent(run_id="run_1", event_type="process.readied")
                        ],
                    ),
                ],
            )
        )
        ghost = await self.journal.get_command_by_idempotency(
            run_id="run_1", idempotency_key="ghost-ready"
        )
        self.assertIsNone(ghost)
        loaded = await self.journal.load(run_id="run_1")
        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0].units[0].command.command_type, "run.create")


if __name__ == "__main__":
    unittest.main()
