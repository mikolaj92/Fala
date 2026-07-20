"""JsonlJournal + TeeJournal tests (PR9 / #89)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from fala.journal import (
    ClaimRequest,
    CommandUnit,
    InMemoryJournal,
    JournalBatch,
    JsonlJournal,
    StateFact,
    TeeJournal,
    append_line_durable,
    encode_journal_line,
    repair_torn_jsonl,
)
from fala.models import JournalConfig
from fala.runtime import open_journal
from fala.runtime_backend import Process, ProcessStatus, RuntimeCommand, RuntimeEvent


def _batch(key: str = "k1", run_id: str = "run_1") -> JournalBatch:
    cmd = RuntimeCommand(
        run_id=run_id,
        command_type="run.create",
        idempotency_key=key,
    )
    return JournalBatch(
        run_id=run_id,
        units=[
            CommandUnit(
                command=cmd,
                events=[RuntimeEvent(run_id=run_id, event_type="run.created")],
                facts=[
                    StateFact(
                        entity="run",
                        op="upsert",
                        key={"id": run_id},
                        body={"id": run_id, "status": "created"},
                    )
                ],
            )
        ],
    )


class JsonlJournalTests(unittest.IsolatedAsyncioTestCase):
    async def test_append_durable_and_reload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "j.journal.jsonl"
            journal = JsonlJournal(path)
            first = await journal.append_batch(_batch("create"))
            self.assertFalse(first.replayed)
            self.assertEqual(first.batch.journal_seq, 1)
            self.assertTrue(path.exists())
            self.assertTrue(path.read_text().endswith("\n"))

            replay = await journal.append_batch(_batch("create"))
            self.assertTrue(replay.replayed)

            reopened = JsonlJournal(path)
            events = await reopened.list_events(run_id="run_1")
            self.assertEqual(len(events), 1)
            loaded = await reopened.load(run_id="run_1")
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[0].journal_seq, 1)

    async def test_torn_line_truncated_on_open(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "torn.jsonl"
            journal = JsonlJournal(path)
            await journal.append_batch(_batch("ok"))
            # Append a torn partial line without newline.
            with path.open("ab") as handle:
                handle.write(b'{"v":1,"kind":"journal_batch","batch":{"run_id":')
            self.assertTrue(repair_torn_jsonl(path))
            reopened = JsonlJournal(path)
            loaded = await reopened.load()
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[0].units[0].command.idempotency_key, "ok")

    async def test_crash_before_fsync_line_absent(self) -> None:
        """Simulate mid-write: partial line without newline must not load as a batch."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "crash.jsonl"
            good = _batch("good")
            # Manually write one durable line then a torn second line.
            j = JsonlJournal(path)
            result = await j.append_batch(good)
            line = encode_journal_line(result.batch)
            # Replace file: good line + partial
            path.write_bytes(line.encode("utf-8") + b'{"v":1,"batch":{"partial"')
            reopened = JsonlJournal(path)
            loaded = await reopened.load()
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[0].units[0].command.idempotency_key, "good")

    async def test_claim_persists_to_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "claim.jsonl"
            journal = JsonlJournal(path)
            journal.seed_process(
                Process(
                    id="p1",
                    run_id="run_1",
                    process_type="effector",
                    status=ProcessStatus.ready,
                )
            )
            claimed = await journal.claim_next(
                ClaimRequest(worker_id="w", run_id="run_1", lease_seconds=30)
            )
            self.assertIsNotNone(claimed.process)
            reopened = JsonlJournal(path)
            events = await reopened.list_events(run_id="run_1")
            self.assertTrue(any(e.event_type == "process.claimed" for e in events))

    async def test_open_journal_jsonl_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "cfg.jsonl")
            opened = open_journal(JournalConfig(kind="jsonl", path=path))
            self.assertIsInstance(opened, JsonlJournal)
            self.assertTrue(opened.runtime_uri.startswith("jsonl://"))

    async def test_tee_mirrors_to_secondary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            primary = JsonlJournal(Path(tmp) / "primary.jsonl")
            secondary = InMemoryJournal(stream_id="memory://secondary")
            tee = TeeJournal(primary, secondary)
            await tee.append_batch(_batch("tee-key"))
            sec_events = await secondary.list_events(run_id="run_1")
            self.assertEqual(len(sec_events), 1)
            # Replay on tee should not double-write secondary
            await tee.append_batch(_batch("tee-key"))
            sec_events2 = await secondary.list_events(run_id="run_1")
            self.assertEqual(len(sec_events2), 1)

    def test_append_line_durable_helper(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "d.jsonl"
            append_line_durable(path, '{"a":1}\n')
            append_line_durable(path, '{"b":2}\n')
            self.assertEqual(path.read_text().count("\n"), 2)


if __name__ == "__main__":
    unittest.main()
