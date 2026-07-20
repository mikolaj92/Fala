"""Tests for open_journal / from_journal constructors (PR6 / #78)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from fala import (
    AutonomousCorrelator,
    InMemoryJournal,
    JournalBackedBackend,
    JournalConfig,
    open_journal,
)
from fala.models import ReactionStoreConfig, RuntimeBackendConfig, RuntimeConfigSpec
from fala.runtime_backend import Run


class JournalConstructorTests(unittest.IsolatedAsyncioTestCase):
    def test_open_journal_memory(self) -> None:
        journal = open_journal(JournalConfig(kind="memory"))
        self.assertIsInstance(journal, InMemoryJournal)
        self.assertTrue(journal.runtime_uri.startswith("memory://"))

    def test_open_journal_sqlite_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "x.sqlite"
            journal = open_journal(path)
            self.assertTrue(journal.runtime_uri.startswith("sqlite://"))
            self.assertIn("x.sqlite", journal.runtime_uri)

    def test_open_journal_from_runtime_config_backend_alias(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "alias.sqlite")
            cfg = RuntimeConfigSpec(
                backend=RuntimeBackendConfig(kind="sqlite", path=path),
                reaction_store=ReactionStoreConfig(kind="filesystem", root=tmp),
            )
            journal = open_journal(cfg)
            self.assertTrue(journal.runtime_uri.startswith("sqlite://"))

    def test_open_journal_from_runtime_config_journal_field(self) -> None:
        cfg = RuntimeConfigSpec(
            journal=JournalConfig(kind="memory"),
            reaction_store=ReactionStoreConfig(kind="filesystem", root="/tmp"),
        )
        journal = open_journal(cfg)
        self.assertIsInstance(journal, InMemoryJournal)

    async def test_from_journal_creates_run(self) -> None:
        runtime = AutonomousCorrelator.from_journal(JournalConfig(kind="memory"))
        self.assertIsInstance(runtime.backend, JournalBackedBackend)
        self.assertIsNotNone(runtime.journal)
        self.assertTrue(runtime.runtime_uri.startswith("memory://"))
        run, submission = await runtime.create_run(
            Run(id="run_j"),
            idempotency_key="create",
        )
        self.assertEqual(run.id, "run_j")
        self.assertFalse(submission.replayed)

    async def test_sqlite_shim_matches_from_journal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "shim.sqlite"
            a = AutonomousCorrelator.sqlite(path)
            b = AutonomousCorrelator.from_journal(path)
            self.assertTrue(a.runtime_uri.startswith("sqlite://"))
            self.assertTrue(b.runtime_uri.startswith("sqlite://"))
            await a.create_run(Run(id="run_a"), idempotency_key="k")
            loaded = await b.backend.get_run(run_id="run_a")
            # separate journal instances → separate DBs unless same path
            a2 = AutonomousCorrelator.from_journal(path)
            loaded = await a2.backend.get_run(run_id="run_a")
            self.assertIsNotNone(loaded)


if __name__ == "__main__":
    unittest.main()
