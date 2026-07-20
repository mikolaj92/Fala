"""Journal conformance tests for InMemoryJournal (PR3 / #75)."""

from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timedelta, timezone

from fala.journal import (
    ClaimRequest,
    CommandUnit,
    InMemoryJournal,
    JournalBatch,
    StateFact,
)
from fala.runtime_backend import Process, ProcessStatus, RuntimeCommand, RuntimeEvent


def _now() -> datetime:
    return datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)


def _cmd(
    *,
    run_id: str = "run_1",
    command_type: str = "run.create",
    key: str = "k1",
) -> RuntimeCommand:
    return RuntimeCommand(
        run_id=run_id,
        command_type=command_type,
        idempotency_key=key,
    )


def _evt(*, run_id: str = "run_1", event_type: str = "run.created") -> RuntimeEvent:
    return RuntimeEvent(run_id=run_id, event_type=event_type)


class InMemoryJournalTests(unittest.IsolatedAsyncioTestCase):
    async def test_append_assigns_sequences_and_idempotent_replay(self) -> None:
        journal = InMemoryJournal()
        cmd = _cmd(key="create_run")
        batch = JournalBatch(
            run_id="run_1",
            units=[
                CommandUnit(
                    command=cmd,
                    events=[_evt()],
                    facts=[
                        StateFact(
                            entity="run",
                            op="upsert",
                            key={"id": "run_1"},
                            body={"id": "run_1", "status": "created"},
                        )
                    ],
                )
            ],
        )
        first = await journal.append_batch(batch)
        self.assertFalse(first.replayed)
        self.assertEqual(first.units[0].events[0].sequence, 1)
        self.assertEqual(first.units[0].events[0].command_id, cmd.id)
        self.assertEqual(first.batch.journal_seq, 1)

        second = await journal.append_batch(batch)
        self.assertTrue(second.replayed)
        self.assertEqual(second.units[0].events, [])

        events = await journal.list_events(run_id="run_1")
        self.assertEqual(len(events), 1)

    async def test_multi_unit_batch_atomic_and_load(self) -> None:
        journal = InMemoryJournal()
        complete = _cmd(command_type="process.complete", key="complete:p1")
        ready = _cmd(command_type="process.ready", key="process.ready:p2")
        batch = JournalBatch(
            run_id="run_1",
            units=[
                CommandUnit(
                    command=complete,
                    events=[_evt(event_type="process.completed")],
                    facts=[
                        StateFact(
                            entity="process",
                            op="upsert",
                            key={"id": "p1"},
                            body={
                                "id": "p1",
                                "run_id": "run_1",
                                "process_type": "effector",
                                "status": "succeeded",
                            },
                        )
                    ],
                ),
                CommandUnit(
                    command=ready,
                    events=[_evt(event_type="process.readied")],
                    facts=[
                        StateFact(
                            entity="process",
                            op="upsert",
                            key={"id": "p2"},
                            body={
                                "id": "p2",
                                "run_id": "run_1",
                                "process_type": "effector",
                                "status": "ready",
                            },
                        )
                    ],
                ),
            ],
        )
        result = await journal.append_batch(batch)
        self.assertFalse(result.replayed)
        self.assertEqual(len(result.units), 2)
        events = await journal.list_events(run_id="run_1")
        self.assertEqual([e.sequence for e in events], [1, 2])
        loaded = await journal.load(run_id="run_1")
        self.assertEqual(len(loaded), 1)
        self.assertEqual(len(loaded[0].units), 2)
        self.assertEqual(journal.get_process("p2").status, ProcessStatus.ready)

    async def test_claim_next_claims_ready_process(self) -> None:
        journal = InMemoryJournal()
        process = Process(
            id="proc_a",
            run_id="run_1",
            process_type="effector",
            status=ProcessStatus.ready,
            attempt=0,
            max_attempts=2,
        )
        journal.seed_process(process)
        result = await journal.claim_next(
            ClaimRequest(worker_id="w1", run_id="run_1", lease_seconds=30)
        )
        self.assertIsNotNone(result.process)
        assert result.process is not None
        self.assertEqual(result.process.status, ProcessStatus.running)
        self.assertEqual(result.process.lease_owner, "w1")
        self.assertEqual(result.process.attempt, 1)
        self.assertIsNotNone(result.batch)
        events = await journal.list_events(run_id="run_1")
        self.assertEqual(events[0].event_type, "process.claimed")

        idle = await journal.claim_next(
            ClaimRequest(worker_id="w2", run_id="run_1", lease_seconds=30)
        )
        self.assertIsNone(idle.process)

    async def test_claim_next_reaps_expired_lease_then_claims(self) -> None:
        journal = InMemoryJournal()
        now = datetime.now(timezone.utc)
        dead = Process(
            id="dead",
            run_id="run_1",
            process_type="effector",
            status=ProcessStatus.running,
            attempt=2,
            max_attempts=2,
            lease_owner="old",
            lease_expires_at=now - timedelta(seconds=5),
        )
        ready = Process(
            id="ready",
            run_id="run_1",
            process_type="effector",
            status=ProcessStatus.ready,
            attempt=0,
            max_attempts=2,
        )
        journal.seed_process(dead)
        journal.seed_process(ready)
        result = await journal.claim_next(
            ClaimRequest(worker_id="w1", run_id="run_1", lease_seconds=30)
        )
        self.assertIsNotNone(result.batch)
        assert result.batch is not None
        types = [u.command.command_type for u in result.batch.units]
        self.assertEqual(types, ["process.fail", "process.claim"])
        self.assertEqual(result.process.id, "ready")
        self.assertEqual(journal.get_process("dead").status, ProcessStatus.failed)

    async def test_recover_entities_from_batches(self) -> None:
        journal = InMemoryJournal()
        await journal.append_batch(
            JournalBatch(
                run_id="run_1",
                units=[
                    CommandUnit(
                        command=_cmd(key="p"),
                        events=[_evt()],
                        facts=[
                            StateFact(
                                entity="process",
                                op="upsert",
                                key={"id": "p9"},
                                body={
                                    "id": "p9",
                                    "run_id": "run_1",
                                    "process_type": "effector",
                                    "status": "pending",
                                },
                            )
                        ],
                    )
                ],
            )
        )
        recovered = journal.recover_entities()
        self.assertIn("p9", recovered["process"])

    async def test_get_command_by_idempotency(self) -> None:
        journal = InMemoryJournal()
        cmd = _cmd(key="unique")
        await journal.append_batch(
            JournalBatch(
                run_id="run_1",
                units=[CommandUnit(command=cmd, events=[_evt()])],
            )
        )
        found = await journal.get_command_by_idempotency(
            run_id="run_1", idempotency_key="unique"
        )
        self.assertIsNotNone(found)
        assert found is not None
        self.assertEqual(found.id, cmd.id)


if __name__ == "__main__":
    unittest.main()
