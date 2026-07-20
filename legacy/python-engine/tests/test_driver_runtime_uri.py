"""Driver runtime_uri / separate journal tests (PR7 / #79)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from fala import (
    AutonomousCorrelator,
    InMemoryJournal,
    JournalBackedBackend,
)
from fala.driver import backend_runtime_uri
from fala.runtime import _default_impulse_reaction_root
from fala.runtime_backend import Correlator


class DriverRuntimeUriTests(unittest.IsolatedAsyncioTestCase):
    def test_backend_runtime_uri_from_journal_backed(self) -> None:
        backend = JournalBackedBackend(InMemoryJournal())
        self.assertTrue(backend_runtime_uri(backend).startswith("memory://"))

    def test_backend_runtime_uri_from_correlator(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "db.sqlite"
            backend = Correlator(path)
            uri = backend_runtime_uri(backend)
            self.assertTrue(uri.startswith("sqlite://"))
            self.assertIn("db.sqlite", uri)

    def test_reaction_root_from_sqlite_journal_backend(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.sqlite"
            runtime = AutonomousCorrelator.from_journal(path)
            root = _default_impulse_reaction_root(runtime.backend)
            self.assertEqual(root, path.resolve().parent / "reactions")


if __name__ == "__main__":
    unittest.main()
